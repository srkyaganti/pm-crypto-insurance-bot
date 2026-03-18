#!/bin/bash
# Paper trading analysis script
# Usage: ./tools/analyze_paper.sh [logfile]
#   Default logfile: logs/paper_run.log

LOG="${1:-logs/paper_run.log}"

if [ ! -f "$LOG" ]; then
  echo "Log file not found: $LOG"
  exit 1
fi

# Strip ANSI codes for processing
CLEAN=$(mktemp)
sed 's/\x1b\[[0-9;]*m//g' "$LOG" > "$CLEAN"
trap "rm -f $CLEAN" EXIT

echo "========================================="
echo "  Paper Trading Analysis"
echo "========================================="

# Time range
FIRST_TS=$(grep -m1 '^20' "$CLEAN" | cut -c1-19)
LAST_TS=$(tail -20 "$CLEAN" | grep '^20' | tail -1 | cut -c1-19)
echo "Period: $FIRST_TS → $LAST_TS"
echo ""

# Markets resolved
RESOLVED=$(grep 'market completed.*archived' "$CLEAN" | wc -l | tr -d ' ')
echo "Markets resolved: $RESOLVED"

# Paper trades
TRADES=$(grep 'PAPER_TRADE' "$CLEAN" | wc -l | tr -d ' ')
echo "Paper trades triggered: $TRADES"
echo ""

# Win/Loss
WINS=$(grep 'market_result=win' "$CLEAN" | wc -l | tr -d ' ')
LOSSES=$(grep 'market_result=loss' "$CLEAN" | wc -l | tr -d ' ')
NO_POS=$(grep 'market_result=no_position' "$CLEAN" | wc -l | tr -d ' ')
echo "--- Results ---"
echo "Wins:        $WINS"
echo "Losses:      $LOSSES"
echo "No position: $NO_POS"
if [ "$((WINS + LOSSES))" -gt 0 ]; then
  WIN_RATE=$(awk "BEGIN {printf \"%.1f\", $WINS / ($WINS + $LOSSES) * 100}")
  echo "Win rate:    ${WIN_RATE}%"
fi
echo ""

# PnL
echo "--- PnL ---"
grep 'market_result=win\|market_result=loss' "$CLEAN" | grep -o 'paper_pnl=[^ ]*' | sed 's/paper_pnl=//' | awk '
  BEGIN {total=0; count=0}
  {total+=$1; count++; pnls[count]=$1}
  END {
    if (count > 0) {
      printf "Total PnL:   $%.2f\n", total
      printf "Avg per trade: $%.2f\n", total/count
      printf "Trades: "
      for (i=1; i<=count; i++) printf "$%.2f ", pnls[i]
      printf "\n"
    }
  }'
echo ""

# Per-symbol breakdown
echo "--- Per Symbol ---"
for SYMBOL in BTC ETH SOL; do
  SYM_WINS=$(grep "market_result=win" "$CLEAN" | grep "symbol=$SYMBOL\b\|slug=.*$(echo $SYMBOL | tr '[:upper:]' '[:lower:]')" | wc -l | tr -d ' ')
  SYM_LOSSES=$(grep "market_result=loss" "$CLEAN" | grep "symbol=$SYMBOL\b\|slug=.*$(echo $SYMBOL | tr '[:upper:]' '[:lower:]')" | wc -l | tr -d ' ')
  SYM_TRADES=$(grep 'PAPER_TRADE' "$CLEAN" | grep "symbol=$SYMBOL" | wc -l | tr -d ' ')
  if [ "$SYM_TRADES" -gt 0 ] || [ "$SYM_WINS" -gt 0 ] || [ "$SYM_LOSSES" -gt 0 ]; then
    echo "  $SYMBOL: $SYM_TRADES triggers, $SYM_WINS wins, $SYM_LOSSES losses"
  fi
done
echo ""

# Per window type
echo "--- Per Window ---"
for W in 5m 15m; do
  W_WINS=$(grep "market_result=win" "$CLEAN" | grep "slug=.*-${W}-" | wc -l | tr -d ' ')
  W_LOSSES=$(grep "market_result=loss" "$CLEAN" | grep "slug=.*-${W}-" | wc -l | tr -d ' ')
  W_TRADES=$(grep 'PAPER_TRADE' "$CLEAN" | grep "slug=.*-${W}-" | wc -l | tr -d ' ')
  if [ "$W_TRADES" -gt 0 ] || [ "$W_WINS" -gt 0 ] || [ "$W_LOSSES" -gt 0 ]; then
    echo "  $W: $W_TRADES triggers, $W_WINS wins, $W_LOSSES losses"
  fi
done
echo ""

# Gate distribution
echo "--- Gate Rejections ---"
grep 'reject_reason=' "$CLEAN" | grep -o 'reject_reason=[^ ]*' | sed 's/reject_reason=//' | sort | uniq -c | sort -rn
echo ""

# Liquidation flag health
LIQ_TRUE=$(grep -o 'liquidation_active=true' "$CLEAN" | wc -l | tr -d ' ')
LIQ_FALSE=$(grep -o 'liquidation_active=false' "$CLEAN" | wc -l | tr -d ' ')
LIQ_TOTAL=$((LIQ_TRUE + LIQ_FALSE))
if [ "$LIQ_TOTAL" -gt 0 ]; then
  LIQ_PCT=$(awk "BEGIN {printf \"%.1f\", $LIQ_TRUE / $LIQ_TOTAL * 100}")
  echo "--- Liquidation Flag ---"
  echo "True: $LIQ_TRUE ($LIQ_PCT%), False: $LIQ_FALSE"
  echo ""
fi

# PM confidence distribution for gate3 rejections
echo "--- PM Confidence (gate3 rejections, top 10) ---"
grep 'gate3_pm_confidence' "$CLEAN" | grep -o 'pm_confidence=[^ ]*' | sed 's/pm_confidence=//' | sort -rn | head -20 | sort | uniq -c | sort -rn | head -10
echo ""

# Loss details (if any)
if [ "$LOSSES" -gt 0 ]; then
  echo "--- LOSS DETAILS ---"
  grep 'market_result=loss' "$CLEAN"
  echo ""
fi

echo "========================================="
