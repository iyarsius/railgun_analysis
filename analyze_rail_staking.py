#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import requests
from eth_utils import keccak
from matplotlib.ticker import FuncFormatter


getcontext().prec = 60

RPC_URL = "https://ethereum-rpc.publicnode.com"
CONTRACT_ADDRESS = "0xee6a649aa3766bd117e12c161726b693a1b2ee20"
CONTRACT_CREATION_TX = "0x81d86b83242d3c0f2c6d7d9d9b15768323d4b09bda2d3747536868d893bd4a26"
DEFAULT_START_DATE = date(2025, 1, 1)
FOCUS_START = datetime(2026, 4, 5, tzinfo=timezone.utc)
FOCUS_END = datetime(2026, 4, 10, 23, 59, 59, tzinfo=timezone.utc)
CHUNK_SIZE = 20_000
BLOCK_TIMESTAMP_CACHE = Path("cache/block_timestamps.json")
STAKE_GETTER_CACHE = Path("cache/stake_getter.json")
LOG_CACHE_DIR = Path("cache/logs")
OUTPUT_DIR = Path("output")
STAKE_LOCKTIME_SECONDS = 30 * 24 * 60 * 60

EVENT_SIGNATURES = {
    "Stake": "Stake(address,uint256,uint256)",
    "Unlock": "Unlock(address,uint256)",
    "Claim": "Claim(address,uint256)",
}

FUNCTION_SIGNATURES = {
    "stakingToken": "stakingToken()",
    "decimals": "decimals()",
    "stakes": "stakes(address,uint256)",
}


@dataclass
class Operation:
    timestamp: int
    block_number: int
    tx_hash: str
    log_index: int
    op_type: str
    account: str
    stake_id: int
    amount_raw: int
    amount_token: Decimal
    source: str = "event_log"

    @property
    def dt(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc)


class RpcClient:
    def __init__(self, rpc_url: str) -> None:
        self.rpc_url = rpc_url
        self.session = requests.Session()
        self.request_id = 0
        self.block_timestamp_cache = self._load_json(BLOCK_TIMESTAMP_CACHE, {})
        self.stake_getter_cache = self._load_json(STAKE_GETTER_CACHE, {})

    def _load_json(self, path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        return json.loads(path.read_text())

    def _save_json(self, path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    def rpc(self, method: str, params: list[Any]) -> Any:
        for attempt in range(8):
            self.request_id += 1
            payload = {
                "jsonrpc": "2.0",
                "id": self.request_id,
                "method": method,
                "params": params,
            }
            try:
                response = self.session.post(self.rpc_url, json=payload, timeout=60)
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise RuntimeError(f"HTTP {response.status_code}: {response.text[:200]}")
                response.raise_for_status()
                decoded = response.json()
                if "error" in decoded:
                    raise RuntimeError(f"RPC error: {decoded['error']}")
                return decoded["result"]
            except Exception:
                if attempt == 7:
                    raise
                time.sleep(min(2 ** attempt, 20))
        raise RuntimeError("Unreachable retry loop")

    def get_block_number(self) -> int:
        return int(self.rpc("eth_blockNumber", []), 16)

    def get_creation_block(self) -> int:
        receipt = self.rpc("eth_getTransactionReceipt", [CONTRACT_CREATION_TX])
        if receipt is None:
            raise RuntimeError("Creation tx receipt not found")
        return int(receipt["blockNumber"], 16)

    def eth_call(self, to: str, data: str) -> str:
        return self.rpc("eth_call", [{"to": to, "data": data}, "latest"])

    def get_block_timestamp(self, block_number: int) -> int:
        key = str(block_number)
        if key in self.block_timestamp_cache:
            return int(self.block_timestamp_cache[key])
        result = self.rpc("eth_getBlockByNumber", [hex(block_number), False])
        timestamp = int(result["timestamp"], 16)
        self.block_timestamp_cache[key] = timestamp
        if len(self.block_timestamp_cache) % 100 == 0:
            self._save_json(BLOCK_TIMESTAMP_CACHE, self.block_timestamp_cache)
        return timestamp

    def flush_caches(self) -> None:
        self._save_json(BLOCK_TIMESTAMP_CACHE, self.block_timestamp_cache)
        self._save_json(STAKE_GETTER_CACHE, self.stake_getter_cache)


def decode_uint256(hex_data: str) -> int:
    return int(hex_data, 16)


def decode_topic_address(topic: str) -> str:
    return "0x" + topic[-40:]


def decode_topic_uint256(topic: str) -> int:
    return int(topic, 16)


def decode_address_result(hex_data: str) -> str:
    return "0x" + hex_data[-40:]


def decode_uint_result(hex_data: str) -> int:
    return int(hex_data, 16)


def event_topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


def function_selector(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()[:8]


def encode_stakes_call(account: str, stake_id: int) -> str:
    selector = function_selector(FUNCTION_SIGNATURES["stakes"])
    account_word = account[2:].rjust(64, "0")
    stake_word = hex(stake_id)[2:].rjust(64, "0")
    return selector + account_word + stake_word


def decode_stake_struct(hex_data: str) -> dict[str, int | str]:
    payload = hex_data[2:]
    words = [payload[index:index + 64] for index in range(0, len(payload), 64)]
    if len(words) < 5:
        raise RuntimeError(f"Unexpected stakes() return data length: {len(words)} words")
    return {
        "delegate": "0x" + words[0][-40:],
        "amount": int(words[1], 16),
        "staketime": int(words[2], 16),
        "locktime": int(words[3], 16),
        "claimed_time": int(words[4], 16),
    }


def stake_cache_key(account: str, stake_id: int) -> str:
    return f"{account.lower()}:{stake_id}"


def chunk_cache_path(event_name: str, from_block: int, to_block: int) -> Path:
    return LOG_CACHE_DIR / f"{event_name}_{from_block}_{to_block}.json"


def fetch_logs(client: RpcClient, event_name: str, topic0: str, from_block: int, to_block: int) -> list[dict[str, Any]]:
    cache_path = chunk_cache_path(event_name, from_block, to_block)
    if cache_path.exists():
        return json.loads(cache_path.read_text())

    params = [{
        "address": CONTRACT_ADDRESS,
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
        "topics": [topic0],
    }]
    logs = client.rpc("eth_getLogs", params)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(logs))
    return logs


def fetch_all_event_logs(client: RpcClient, event_name: str, topic0: str, from_block: int, to_block: int) -> list[dict[str, Any]]:
    all_logs: list[dict[str, Any]] = []
    current = from_block
    while current <= to_block:
        end = min(current + CHUNK_SIZE - 1, to_block)
        all_logs.extend(fetch_logs(client, event_name, topic0, current, end))
        current = end + 1
    all_logs.sort(key=lambda item: (int(item["blockNumber"], 16), int(item["logIndex"], 16)))
    return all_logs


def format_amount(raw_amount: int, decimals: int) -> Decimal:
    return Decimal(raw_amount) / (Decimal(10) ** decimals)


def day_range(start_day: date, end_day: date) -> list[date]:
    days: list[date] = []
    current = start_day
    while current <= end_day:
        days.append(current)
        current += timedelta(days=1)
    return days


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def plot_daily_series(path: Path, title: str, dates: list[date], values: list[Decimal], ylabel: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(13, 6))
    ax = plt.gca()
    ax.plot(dates, [float(value) for value in values], linewidth=2)
    plt.title(title)
    plt.xlabel("Date (UTC)")
    plt.ylabel(ylabel)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    ax.ticklabel_format(axis="y", style="plain", useOffset=False)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:,.0f}"))
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()


def build_series(days: list[date], deltas: dict[date, Decimal], initial_value: Decimal = Decimal(0)) -> list[Decimal]:
    values: list[Decimal] = []
    running = initial_value
    for day in days:
        running += deltas.get(day, Decimal(0))
        values.append(running)
    return values


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    client = RpcClient(RPC_URL)
    latest_block = client.get_block_number()
    deploy_block = client.get_creation_block()

    staking_token = decode_address_result(client.eth_call(CONTRACT_ADDRESS, function_selector(FUNCTION_SIGNATURES["stakingToken"])))
    decimals = decode_uint_result(client.eth_call(staking_token, function_selector(FUNCTION_SIGNATURES["decimals"])))
    latest_timestamp = client.get_block_timestamp(latest_block)
    latest_day = datetime.fromtimestamp(latest_timestamp, tz=timezone.utc).date()

    raw_logs: dict[str, list[dict[str, Any]]] = {}
    topics = {name: event_topic(signature) for name, signature in EVENT_SIGNATURES.items()}
    for event_name, topic0 in topics.items():
        raw_logs[event_name] = fetch_all_event_logs(client, event_name, topic0, deploy_block, latest_block)

    stake_amounts: dict[tuple[str, int], int] = {}
    stake_amount_sources: dict[tuple[str, int], str] = {}
    stake_ops: list[Operation] = []
    unlock_ops: list[Operation] = []
    claim_ops: list[Operation] = []
    backfilled_rows: list[dict[str, Any]] = []
    all_keys: set[tuple[str, int]] = set()
    unlock_keys_from_logs: set[tuple[str, int]] = set()
    claim_keys_from_logs: set[tuple[str, int]] = set()

    for log in raw_logs["Stake"]:
        account = decode_topic_address(log["topics"][1]).lower()
        stake_id = decode_topic_uint256(log["topics"][2])
        amount_raw = decode_uint256(log["data"])
        block_number = int(log["blockNumber"], 16)
        timestamp = client.get_block_timestamp(block_number)
        op = Operation(
            timestamp=timestamp,
            block_number=block_number,
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            op_type="stake",
            account=account,
            stake_id=stake_id,
            amount_raw=amount_raw,
            amount_token=format_amount(amount_raw, decimals),
        )
        stake_amounts[(account, stake_id)] = amount_raw
        stake_amount_sources[(account, stake_id)] = "stake_event"
        stake_ops.append(op)
        all_keys.add((account, stake_id))

    def get_stake_data(account: str, stake_id: int) -> dict[str, int | str]:
        cache_key = stake_cache_key(account, stake_id)
        if cache_key in client.stake_getter_cache:
            return client.stake_getter_cache[cache_key]
        stake_data = decode_stake_struct(client.eth_call(CONTRACT_ADDRESS, encode_stakes_call(account, stake_id)))
        client.stake_getter_cache[cache_key] = stake_data
        return stake_data

    def resolve_amount_raw(account: str, stake_id: int) -> int:
        key = (account, stake_id)
        if key in stake_amounts:
            return stake_amounts[key]

        stake_data = get_stake_data(account, stake_id)
        amount_raw = int(stake_data["amount"])
        staketime = int(stake_data["staketime"])
        if staketime == 0:
            raise RuntimeError(f"Missing stake amount and stakes() reports empty struct for {account}#{stake_id}")

        stake_amounts[key] = amount_raw
        stake_amount_sources[key] = "stakes_getter_backfill"
        backfilled_rows.append({
            "account": account,
            "stake_id": stake_id,
            "amount_raw": str(amount_raw),
            "amount_rail": f"{format_amount(amount_raw, decimals):.18f}",
            "delegate": stake_data["delegate"],
            "staketime": int(stake_data["staketime"]),
            "locktime": int(stake_data["locktime"]),
            "claimed_time": int(stake_data["claimed_time"]),
            "source": "stakes_getter_backfill",
        })
        all_keys.add(key)
        return amount_raw

    for log in raw_logs["Unlock"]:
        account = decode_topic_address(log["topics"][1]).lower()
        stake_id = decode_topic_uint256(log["topics"][2])
        amount_raw = resolve_amount_raw(account, stake_id)
        block_number = int(log["blockNumber"], 16)
        timestamp = client.get_block_timestamp(block_number)
        op = Operation(
            timestamp=timestamp,
            block_number=block_number,
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            op_type="unlock",
            account=account,
            stake_id=stake_id,
            amount_raw=amount_raw,
            amount_token=format_amount(amount_raw, decimals),
        )
        unlock_ops.append(op)
        unlock_keys_from_logs.add((account, stake_id))
        all_keys.add((account, stake_id))

    for log in raw_logs["Claim"]:
        account = decode_topic_address(log["topics"][1]).lower()
        stake_id = decode_topic_uint256(log["topics"][2])
        amount_raw = resolve_amount_raw(account, stake_id)
        block_number = int(log["blockNumber"], 16)
        timestamp = client.get_block_timestamp(block_number)
        op = Operation(
            timestamp=timestamp,
            block_number=block_number,
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            op_type="claim",
            account=account,
            stake_id=stake_id,
            amount_raw=amount_raw,
            amount_token=format_amount(amount_raw, decimals),
        )
        claim_ops.append(op)
        claim_keys_from_logs.add((account, stake_id))
        all_keys.add((account, stake_id))

    for account, stake_id in sorted(all_keys):
        stake_data = get_stake_data(account, stake_id)
        amount_raw = resolve_amount_raw(account, stake_id)
        amount_token = format_amount(amount_raw, decimals)
        locktime = int(stake_data["locktime"])
        claimed_time = int(stake_data["claimed_time"])

        if locktime != 0 and (account, stake_id) not in unlock_keys_from_logs:
            unlock_ops.append(
                Operation(
                    timestamp=locktime - STAKE_LOCKTIME_SECONDS,
                    block_number=0,
                    tx_hash="backfilled_from_stakes_getter",
                    log_index=-1,
                    op_type="unlock",
                    account=account,
                    stake_id=stake_id,
                    amount_raw=amount_raw,
                    amount_token=amount_token,
                    source="stakes_getter_backfill",
                )
            )

        if claimed_time != 0 and (account, stake_id) not in claim_keys_from_logs:
            claim_ops.append(
                Operation(
                    timestamp=claimed_time,
                    block_number=0,
                    tx_hash="backfilled_from_stakes_getter",
                    log_index=-1,
                    op_type="claim",
                    account=account,
                    stake_id=stake_id,
                    amount_raw=amount_raw,
                    amount_token=amount_token,
                    source="stakes_getter_backfill",
                )
            )

    unlock_ops.sort(key=lambda op: (op.timestamp, op.block_number, op.log_index, op.account, op.stake_id))
    claim_ops.sort(key=lambda op: (op.timestamp, op.block_number, op.log_index, op.account, op.stake_id))

    start_day = DEFAULT_START_DATE
    days = day_range(start_day, latest_day)

    stake_daily_in: dict[date, Decimal] = defaultdict(lambda: Decimal(0))
    pending_deltas: dict[date, Decimal] = defaultdict(lambda: Decimal(0))
    claimable_deltas: dict[date, Decimal] = defaultdict(lambda: Decimal(0))
    pending_initial = Decimal(0)
    claimable_initial = Decimal(0)

    for op in stake_ops:
        day = op.dt.date()
        if day >= start_day:
            stake_daily_in[day] += op.amount_token

    for op in unlock_ops:
        unlock_day = op.dt.date()
        if unlock_day < start_day:
            pending_initial += op.amount_token
        else:
            pending_deltas[unlock_day] += op.amount_token

        claimable_start = datetime.fromtimestamp(op.timestamp + STAKE_LOCKTIME_SECONDS, tz=timezone.utc)
        claimable_day = claimable_start.date()
        if claimable_day < start_day:
            claimable_initial += op.amount_token
        else:
            claimable_deltas[claimable_day] += op.amount_token

    for op in claim_ops:
        claim_day = op.dt.date()
        if claim_day < start_day:
            pending_initial -= op.amount_token
            claimable_initial -= op.amount_token
        else:
            pending_deltas[claim_day] -= op.amount_token
            claimable_deltas[claim_day] -= op.amount_token

    stake_rows: list[dict[str, Any]] = []
    pending_rows: list[dict[str, Any]] = []
    claimable_rows: list[dict[str, Any]] = []

    pending_series = build_series(days, pending_deltas, pending_initial)
    claimable_series = build_series(days, claimable_deltas, claimable_initial)

    stake_values: list[Decimal] = []
    for idx, day in enumerate(days):
        stake_value = stake_daily_in.get(day, Decimal(0))
        stake_values.append(stake_value)
        stake_rows.append({"date_utc": day.isoformat(), "stake_in_rail": f"{stake_value:.18f}"})
        pending_rows.append({"date_utc": day.isoformat(), "pending_claim_rail": f"{pending_series[idx]:.18f}"})
        claimable_rows.append({"date_utc": day.isoformat(), "claimable_rail": f"{claimable_series[idx]:.18f}"})

    write_csv(OUTPUT_DIR / "stake_in_daily.csv", stake_rows, ["date_utc", "stake_in_rail"])
    write_csv(OUTPUT_DIR / "pending_claim_daily.csv", pending_rows, ["date_utc", "pending_claim_rail"])
    write_csv(OUTPUT_DIR / "claimable_daily.csv", claimable_rows, ["date_utc", "claimable_rail"])

    plot_daily_series(OUTPUT_DIR / "stake_in_daily.png", "RAIL stake entrant par jour", days, stake_values, "RAIL")
    plot_daily_series(OUTPUT_DIR / "pending_claim_daily.png", "RAIL pending claim par jour (stock fin de jour)", days, pending_series, "RAIL")
    plot_daily_series(OUTPUT_DIR / "claimable_daily.png", "RAIL claimable par jour (stock fin de jour)", days, claimable_series, "RAIL")

    focus_ops = [
        *[op for op in unlock_ops if FOCUS_START <= op.dt <= FOCUS_END],
        *[op for op in claim_ops if FOCUS_START <= op.dt <= FOCUS_END],
        *[op for op in stake_ops if FOCUS_START <= op.dt <= FOCUS_END],
    ]
    focus_ops.sort(key=lambda op: (-op.amount_raw, op.timestamp, op.log_index))
    focus_rows = []
    for op in focus_ops:
        focus_rows.append({
            "datetime_utc": op.dt.isoformat().replace("+00:00", "Z"),
            "operation_type": op.op_type,
            "account": op.account,
            "stake_id": op.stake_id,
            "amount_rail": f"{op.amount_token:.18f}",
            "tx_hash": op.tx_hash,
            "block_number": op.block_number,
            "source": op.source,
        })
    write_csv(
        OUTPUT_DIR / "top_operations_2026-04-05_2026-04-10.csv",
        focus_rows,
        ["datetime_utc", "operation_type", "account", "stake_id", "amount_rail", "tx_hash", "block_number", "source"],
    )

    def top_rows(ops: list[Operation], limit: int = 15) -> list[dict[str, Any]]:
        items = [op for op in ops if FOCUS_START <= op.dt <= FOCUS_END]
        items.sort(key=lambda op: (-op.amount_raw, op.timestamp, op.log_index))
        rows = []
        for op in items[:limit]:
            rows.append({
                "datetime_utc": op.dt.isoformat().replace("+00:00", "Z"),
                "account": op.account,
                "stake_id": op.stake_id,
                "amount_rail": f"{op.amount_token:.18f}",
                "tx_hash": op.tx_hash,
                "block_number": op.block_number,
                "source": op.source,
            })
        return rows

    write_csv(OUTPUT_DIR / "top_unlocks_2026-04-05_2026-04-10.csv", top_rows(unlock_ops), ["datetime_utc", "account", "stake_id", "amount_rail", "tx_hash", "block_number", "source"])
    write_csv(OUTPUT_DIR / "top_claims_2026-04-05_2026-04-10.csv", top_rows(claim_ops), ["datetime_utc", "account", "stake_id", "amount_rail", "tx_hash", "block_number", "source"])
    write_csv(OUTPUT_DIR / "top_stakes_2026-04-05_2026-04-10.csv", top_rows(stake_ops), ["datetime_utc", "account", "stake_id", "amount_rail", "tx_hash", "block_number", "source"])

    history_backfill_rows = []
    for op in [*unlock_ops, *claim_ops]:
        if op.source != "event_log":
            history_backfill_rows.append({
                "datetime_utc": op.dt.isoformat().replace("+00:00", "Z"),
                "operation_type": op.op_type,
                "account": op.account,
                "stake_id": op.stake_id,
                "amount_rail": f"{op.amount_token:.18f}",
                "tx_hash": op.tx_hash,
                "block_number": op.block_number,
                "source": op.source,
            })
    history_backfill_rows.sort(key=lambda row: (row["datetime_utc"], row["operation_type"], row["account"], row["stake_id"]))
    if history_backfill_rows:
        write_csv(
            OUTPUT_DIR / "history_backfills.csv",
            history_backfill_rows,
            ["datetime_utc", "operation_type", "account", "stake_id", "amount_rail", "tx_hash", "block_number", "source"],
        )

    if backfilled_rows:
        write_csv(
            OUTPUT_DIR / "backfilled_stake_amounts.csv",
            backfilled_rows,
            ["account", "stake_id", "amount_raw", "amount_rail", "delegate", "staketime", "locktime", "claimed_time", "source"],
        )

    unlock_focus = [op for op in unlock_ops if FOCUS_START <= op.dt <= FOCUS_END]
    claim_focus = [op for op in claim_ops if FOCUS_START <= op.dt <= FOCUS_END]
    stake_focus = [op for op in stake_ops if FOCUS_START <= op.dt <= FOCUS_END]

    current_pending_total = Decimal(0)
    current_claimable_total = Decimal(0)
    for account, stake_id in sorted(all_keys):
        stake_data = get_stake_data(account, stake_id)
        amount_raw = resolve_amount_raw(account, stake_id)
        if int(stake_data["locktime"]) != 0 and int(stake_data["claimed_time"]) == 0:
            amount_token = format_amount(amount_raw, decimals)
            current_pending_total += amount_token
            if int(stake_data["locktime"]) < latest_timestamp:
                current_claimable_total += amount_token

    focus_pending_delta = sum((op.amount_token for op in unlock_focus), Decimal(0)) - sum((op.amount_token for op in claim_focus), Decimal(0))

    summary = {
        "rpc_url": RPC_URL,
        "contract_address": CONTRACT_ADDRESS,
        "staking_token": staking_token,
        "decimals": decimals,
        "deploy_block": deploy_block,
        "latest_block": latest_block,
        "latest_timestamp_utc": datetime.fromtimestamp(latest_timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "focus_window": {
            "start": FOCUS_START.isoformat().replace("+00:00", "Z"),
            "end": FOCUS_END.isoformat().replace("+00:00", "Z"),
        },
        "focus_counts": {
            "unlocks": len(unlock_focus),
            "claims": len(claim_focus),
            "stakes": len(stake_focus),
        },
        "focus_amounts_rail": {
            "unlock_total": f"{sum((op.amount_token for op in unlock_focus), Decimal(0)):.18f}",
            "claim_total": f"{sum((op.amount_token for op in claim_focus), Decimal(0)):.18f}",
            "stake_total": f"{sum((op.amount_token for op in stake_focus), Decimal(0)):.18f}",
            "net_pending_change": f"{focus_pending_delta:.18f}",
        },
        "current_totals_rail": {
            "pending_claim": f"{current_pending_total:.18f}",
            "claimable_now": f"{current_claimable_total:.18f}",
        },
        "methodology": {
            "unlock_and_claim_amounts": "Reconstructed from Stake(account, stakeID, amount); Unlock and Claim events do not emit amount.",
            "missing_stake_logs_fallback": "If an Unlock/Claim referenced a stake without a corresponding Stake log in the fetched history, amount was backfilled from stakes(account, stakeID).amount.",
            "missing_unlock_or_claim_logs_fallback": "Known stakes were reconciled against stakes(account, stakeID).locktime and claimedTime to backfill missing Unlock/Claim history when needed.",
            "stake_id_scope": "Per-account index in stakes[account].",
            "partial_claim": False,
            "partial_unlock": False,
            "unlock_cancel": False,
        },
        "backfilled_stake_amount_count": len(backfilled_rows),
        "backfilled_history_operation_count": len(history_backfill_rows),
    }
    (OUTPUT_DIR / "summary.json").write_text(json.dumps(summary, indent=2))
    client.flush_caches()


if __name__ == "__main__":
    main()
