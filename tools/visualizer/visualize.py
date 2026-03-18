#!/usr/bin/env python3
"""Visualize Polymarket snapshot data captured by the sampler."""

import argparse
import json
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def parse_args():
    p = argparse.ArgumentParser(description="Visualize Polymarket snapshot data")
    p.add_argument("--input", required=True, help="Path to snapshots.ndjson")
    p.add_argument("--symbol", default="BTC", help="Symbol to visualize (default: BTC)")
    p.add_argument("--output", default=None, help="Output HTML file (default: {symbol}_analysis.html)")
    p.add_argument("--from", dest="from_ts", default=None, help="Start timestamp (ISO format)")
    p.add_argument("--to", dest="to_ts", default=None, help="End timestamp (ISO format)")
    return p.parse_args()


def load_snapshots(path, symbol, from_ts=None, to_ts=None):
    rows = []
    windows_data = {}  # slug -> {ts, yes_mid, no_mid, ...}

    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                snap = json.loads(line)
            except json.JSONDecodeError:
                continue

            ts = datetime.fromisoformat(snap["ts"].replace("Z", "+00:00"))
            if from_ts and ts < from_ts:
                continue
            if to_ts and ts > to_ts:
                continue

            sym_data = snap.get("symbols", {}).get(symbol, {})
            row = {"ts": ts}
            row.update(sym_data)
            rows.append(row)

            for w in snap.get("windows", []):
                if w.get("symbol") != symbol:
                    continue
                slug = w["slug"]
                if slug not in windows_data:
                    windows_data[slug] = {
                        "ts": [],
                        "yes_mid": [],
                        "no_mid": [],
                        "yes_bid": [],
                        "yes_ask": [],
                        "no_bid": [],
                        "no_ask": [],
                        "start_price": w.get("start_price", 0),
                        "end_ts": w.get("end_ts"),
                        "window_min": w.get("window_min", 0),
                    }
                windows_data[slug]["ts"].append(ts)
                windows_data[slug]["yes_mid"].append(w.get("yes_mid", 0))
                windows_data[slug]["no_mid"].append(w.get("no_mid", 0))
                windows_data[slug]["yes_bid"].append(w.get("yes_bid", 0))
                windows_data[slug]["yes_ask"].append(w.get("yes_ask", 0))
                windows_data[slug]["no_bid"].append(w.get("no_bid", 0))
                windows_data[slug]["no_ask"].append(w.get("no_ask", 0))

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("ts").reset_index(drop=True)
    return df, windows_data


def build_window_boundaries(windows_data):
    boundaries = []
    for slug, data in windows_data.items():
        if not data["ts"]:
            continue
        start_ts = min(data["ts"])
        end_ts_str = data.get("end_ts")
        if end_ts_str:
            try:
                end_ts = datetime.fromisoformat(end_ts_str.replace("Z", "+00:00"))
            except Exception:
                end_ts = max(data["ts"])
        else:
            end_ts = max(data["ts"])
        boundaries.append({
            "slug": slug,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "start_price": data["start_price"],
            "window_min": data["window_min"],
        })
    return boundaries


def add_direction_shading(fig, df, windows_data):
    """Shade panel 1 green/red based on whether cl_price is above/below each window's start_price."""
    if df.empty or "cl_price" not in df.columns:
        return

    ts_to_price = dict(zip(df["ts"], df["cl_price"]))

    for slug, data in windows_data.items():
        start_price = data["start_price"]
        if start_price == 0 or not data["ts"]:
            continue

        tlist = data["ts"]
        dirs = ["up" if ts_to_price.get(ts, 0) > start_price else "down" for ts in tlist]

        # Group consecutive same-direction segments into vrects
        i = 0
        while i < len(tlist):
            j = i + 1
            while j < len(tlist) and dirs[j] == dirs[i]:
                j += 1
            color = "rgba(0,160,0,0.10)" if dirs[i] == "up" else "rgba(200,0,0,0.10)"
            fig.add_vrect(
                x0=tlist[i].isoformat(),
                x1=tlist[j - 1].isoformat(),
                fillcolor=color,
                line_width=0,
                row=1, col=1,
            )
            i = j


def make_figure(df, windows_data, symbol):
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.40, 0.35, 0.25],
        vertical_spacing=0.04,
        subplot_titles=[
            f"{symbol} Price: Chainlink vs Spot  (green = above start, red = below)",
            "Polymarket YES / NO Mid Prices",
            "Binance Spot Flow Imbalance",
        ],
    )

    boundaries = build_window_boundaries(windows_data)

    # ── Panel 1: direction shading + Chainlink + Spot + start_price refs ─────
    add_direction_shading(fig, df, windows_data)

    if not df.empty and "cl_price" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["ts"], y=df["cl_price"],
                name="Chainlink", line=dict(color="orange", width=2),
            ),
            row=1, col=1,
        )
    if not df.empty and "spot_mid" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["ts"], y=df["spot_mid"],
                name="Spot Mid", line=dict(color="royalblue", dash="dash", width=1.5),
            ),
            row=1, col=1,
        )
    for b in boundaries:
        fig.add_hline(
            y=b["start_price"],
            line=dict(color="gray", dash="dot", width=1),
            row=1, col=1,
        )

    # ── Panel 2: YES mid (solid) + NO mid (dashed) per slug + 0.96 threshold ─
    colors = [
        "#e41a1c", "#377eb8", "#4daf4a", "#984ea3",
        "#ff7f00", "#a65628", "#f781bf", "#999999",
    ]
    for i, (slug, data) in enumerate(windows_data.items()):
        label = slug[-20:] if len(slug) > 20 else slug
        color = colors[i % len(colors)]
        fig.add_trace(
            go.Scatter(
                x=data["ts"], y=data["yes_mid"],
                name=f"YES {label}", line=dict(color=color, width=1.5),
                legendgroup=slug,
            ),
            row=2, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=data["ts"], y=data["no_mid"],
                name=f"NO  {label}", line=dict(color=color, dash="dash", width=1),
                legendgroup=slug,
            ),
            row=2, col=1,
        )
    fig.add_hline(
        y=0.96,
        line=dict(color="red", dash="dash", width=1),
        annotation_text="0.96",
        annotation_position="right",
        row=2, col=1,
    )

    # ── Panel 3: Flow imbalance 10s / 30s ─────────────────────────────────────
    if not df.empty and "spot_imb_10s" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["ts"], y=df["spot_imb_10s"],
                name="Imbalance 10s", line=dict(color="purple", width=1.5),
            ),
            row=3, col=1,
        )
    if not df.empty and "spot_imb_30s" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["ts"], y=df["spot_imb_30s"],
                name="Imbalance 30s", line=dict(color="magenta", dash="dash", width=1.5),
            ),
            row=3, col=1,
        )
    fig.add_hline(y=0, line=dict(color="black", width=0.5), row=3, col=1)

    # ── Window boundary vertical lines across all panels ─────────────────────
    for b in boundaries:
        fig.add_vline(x=b["start_ts"].isoformat(), line=dict(color="green", dash="dot", width=1))
        fig.add_vline(x=b["end_ts"].isoformat(), line=dict(color="red", dash="dot", width=1))

    fig.update_layout(
        title=f"{symbol} Polymarket Analysis",
        hovermode="x unified",
        height=950,
        legend=dict(orientation="v", x=1.01, y=1),
        margin=dict(r=180),
    )

    # Tight x-axis range based on actual data + range selector for zooming
    all_ts = list(df["ts"]) if not df.empty else []
    for data in windows_data.values():
        all_ts.extend(data["ts"])
    if all_ts:
        ts_min = min(all_ts)
        ts_max = max(all_ts)
        pad = (ts_max - ts_min) * 0.02 if ts_max > ts_min else pd.Timedelta(seconds=5)
        for row in range(1, 4):
            fig.update_xaxes(range=[ts_min - pad, ts_max + pad], row=row, col=1)

    fig.update_xaxes(
        title_text="Time",
        rangeslider=dict(visible=True, thickness=0.06),
        rangeselector=dict(
            buttons=[
                dict(count=30, label="30s", step="second", stepmode="backward"),
                dict(count=1, label="1m", step="minute", stepmode="backward"),
                dict(count=2, label="2m", step="minute", stepmode="backward"),
                dict(count=5, label="5m", step="minute", stepmode="backward"),
                dict(step="all", label="All"),
            ],
            x=0, y=-0.15,
        ),
        row=3, col=1,
    )
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Mid Price", row=2, col=1)
    fig.update_yaxes(title_text="Imbalance", row=3, col=1)

    return fig


def main():
    args = parse_args()

    from_ts = None
    to_ts = None
    if args.from_ts:
        from_ts = datetime.fromisoformat(args.from_ts.replace("Z", "+00:00"))
    if args.to_ts:
        to_ts = datetime.fromisoformat(args.to_ts.replace("Z", "+00:00"))

    output = args.output or f"{args.symbol.lower()}_analysis.html"

    print(f"Loading snapshots from {args.input} for symbol {args.symbol}...")
    df, windows_data = load_snapshots(args.input, args.symbol, from_ts, to_ts)
    print(f"  Loaded {len(df)} ticks, {len(windows_data)} market windows")

    fig = make_figure(df, windows_data, args.symbol)
    fig.write_html(output)
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
