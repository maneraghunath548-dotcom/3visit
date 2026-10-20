from flask import Flask, jsonify, request
import aiohttp
import asyncio
import json
from byte import encrypt_api, Encrypt_ID

# ✅ Naya import — sahi protobuf schema
from proto.AccountPersonalShow_pb2 import AccountPersonalShowInfo

app = Flask(__name__)

# ═══════════════════════════════════════════════════════════════
# ⚙️ CONFIG
# ═══════════════════════════════════════════════════════════════

DEFAULT_VISITS   = 1000       # ✅ URL me amount na ho to itna
MAX_VISITS_LIMIT = 50000      # ✅ Max limit
BATCH_SIZE       = 50         # ✅ Ek baar me 50 requests (429 se bachne ke liye)
BATCH_DELAY      = 2          # ✅ Har batch ke baad 2 sec wait

# ═══════════════════════════════════════════════════════════════
# 📁 TOKEN LOAD
# ═══════════════════════════════════════════════════════════════

def load_tokens(server_name):
    """List format (dict bhi support) dono padh sakta hai."""
    try:
        if server_name == "IND":
            path = "token_ind.json"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            path = "token_br.json"
        elif server_name == "RU":
            path = "token_ru.json"
        else:
            path = "token_bd.json"

        with open(path, "r") as f:
            data = json.load(f)

        tokens = []

        # List format: [{"token": "..."}, ...]
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "token" in item:
                    t = item["token"]
                    if t and t not in ("", "N/A"):
                        tokens.append(t)

        # Dict format: {"tokens": {uid: {"token": ...}}}
        elif isinstance(data, dict) and "tokens" in data:
            for uid, info in data["tokens"].items():
                t = info.get("token")
                if t and t not in ("", "N/A"):
                    tokens.append(t)

        return tokens, len(tokens)
    except Exception as e:
        app.logger.error(f"❌ Token load error for {server_name}: {e}")
        return [], 0


def get_url(server_name):
    if server_name == "IND":
        return "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
    elif server_name in {"BR", "US", "SAC", "NA"}:
        return "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
    elif server_name == "RU":
        return "https://client.ru.freefiremobile.com/GetPlayerPersonalShow"
    else:
        return "https://clientbp.ggpolarbear.com/GetPlayerPersonalShow"


# ═══════════════════════════════════════════════════════════════
# 🎯 PARSE PLAYER INFO (naya schema)
# ═══════════════════════════════════════════════════════════════

def parse_protobuf_response(response_data):
    try:
        show = AccountPersonalShowInfo()
        show.ParseFromString(response_data)

        if not show.HasField("basic_info"):
            app.logger.warning("⚠️ basic_info field missing")
            return None

        b = show.basic_info
        return {
            "uid":      b.account_id if b.HasField("account_id") else 0,
            "nickname": b.nickname if b.HasField("nickname") else "",
            "likes":    b.liked if b.HasField("liked") else 0,
            "region":   b.region if b.HasField("region") else "",
            "level":    b.level if b.HasField("level") else 0,
        }
    except Exception as e:
        app.logger.error(f"❌ Protobuf parsing error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# 🚀 VISIT REQUEST
# ═══════════════════════════════════════════════════════════════

async def visit(session, url, token, uid, data):
    headers = {
        "ReleaseVersion": "OB55",
        "X-GA": "v1 1",
        "Authorization": f"Bearer {token}",
        "Host": url.replace("https://", "").split("/")[0],
    }
    try:
        async with session.post(url, headers=headers, data=data, ssl=False) as resp:
            if resp.status == 200:
                return True, await resp.read()

            print(f"⚠️ Visit HTTP {resp.status} — token {token[:25]}...")

            # ✅ 429 (rate limit) pe 5 sec wait
            if resp.status == 429:
                await asyncio.sleep(5)

            return False, None
    except Exception as e:
        app.logger.error(f"❌ Visit error: {e}")
        return False, None


# ═══════════════════════════════════════════════════════════════
# 🔁 SEND LOOP (custom amount ke saath)
# ═══════════════════════════════════════════════════════════════

async def send_visits(tokens, uid, server_name, target_success=1000):
    url = get_url(server_name)
    connector = aiohttp.TCPConnector(limit=0)

    total_success = 0
    total_sent = 0
    first_success_response = None
    player_info = None
    failed_batches = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        encrypted = encrypt_api("08" + Encrypt_ID(str(uid)) + "1801")
        data = bytes.fromhex(encrypted)

        while total_success < target_success:
            batch_size = min(target_success - total_success, BATCH_SIZE)
            tasks = [
                asyncio.create_task(
                    visit(session, url, tokens[(total_sent + i) % len(tokens)], uid, data)
                )
                for i in range(batch_size)
            ]
            results = await asyncio.gather(*tasks)

            # Pehla successful response se player info parse
            if first_success_response is None:
                for success, response in results:
                    if success and response is not None:
                        first_success_response = response
                        player_info = parse_protobuf_response(response)
                        break

            batch_success = sum(1 for r, _ in results if r)
            total_success += batch_success
            total_sent += batch_size

            print(f"📊 Batch: {batch_size} | Success: {batch_success} | Total: {total_success}/{target_success}")

            # 3 consecutive failed batches pe stop (infinite loop se bachne)
            if batch_success == 0:
                failed_batches += 1
                if failed_batches >= 3:
                    print("⚠️ 3 failed batches — stopping")
                    break
            else:
                failed_batches = 0

            # ✅ Har batch ke baad delay (429 se bachne ke liye)
            if total_success < target_success:
                await asyncio.sleep(BATCH_DELAY)

    return total_success, total_sent, player_info


# ═══════════════════════════════════════════════════════════════
# 🌐 ROUTE: /visit?uid=...&server_name=...&key=...&amount=...
# ═══════════════════════════════════════════════════════════════

@app.route('/visit', methods=['GET'])
def send_visits_query():
    uid         = request.args.get('uid')
    server_name = request.args.get('server_name')
    key         = request.args.get('key')

    # ✅ NEW: amount parameter
    amount = request.args.get('amount', default=DEFAULT_VISITS, type=int)

    # Parameter validation
    if not uid or not server_name or not key:
        return jsonify({"error": "Missing parameters: uid, server_name, key"}), 400

    if key != "MNSTR":
        return jsonify({"error": "Invalid API key"}), 401

    try:
        uid_int = int(uid)
    except ValueError:
        return jsonify({"error": "uid must be an integer"}), 400

    # ✅ Amount clamp karo
    amount = min(max(amount, 1), MAX_VISITS_LIMIT)
    server = server_name.upper()

    # Token load
    tokens, token_count = load_tokens(server)

    if not tokens:
        return jsonify({
            "error": "❌ No valid tokens found for server",
            "available_accounts": 0
        }), 500

    print(f"\n{'='*60}")
    print(f"🚀 UID={uid_int} | Server={server} | Amount={amount} | Tokens={token_count}")
    print(f"{'='*60}")

    total_success, total_sent, player_info = asyncio.run(
        send_visits(tokens, uid_int, server, amount)
    )

    response = {
        "target": amount,
        "success": total_success,
        "failed": amount - total_success,
        "sent": total_sent,
        "available_accounts": token_count,
    }

    if player_info:
        response.update({
            "nickname": player_info.get("nickname", ""),
            "uid":      player_info.get("uid", 0),
            "level":    player_info.get("level", 0),
            "likes":    player_info.get("likes", 0),
            "region":   player_info.get("region", ""),
        })
    else:
        response["player_info"] = None
        response["note"] = "Player info decode nahi hua"

    return jsonify(response), 200


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 VISIT BOT STARTING")
    print(f"   Default visits: {DEFAULT_VISITS}")
    print(f"   Max visits: {MAX_VISITS_LIMIT}")
    print(f"   Batch size: {BATCH_SIZE} | Delay: {BATCH_DELAY}s")
    print("=" * 60)
    app.run(host="0.0.0.0", port=8000)
