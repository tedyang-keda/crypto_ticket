# Binance / OKX Historical Adjustment Verification Report

Date: 2026-07-23 (Asia/Shanghai)

Environment: `ticket` (`/opt/crypto_ticket`, commit `ea27ed7`)

Production host `hn3` was not accessed or changed.

## Scope

The verification covers six official rebase / contract-size actions and one no-action control:

- OKX: `OPENAI-USDT-SWAP`, `ANTHROPIC-USDT-SWAP`, `SPACEX-USDT-SWAP -> SPCX-USDT-SWAP`.
- Binance USD-M: `SPCXUSDT`, `CRWDUSDT`, `KORUUSDT`.
- No-action control: OKX `ZHIPU-USDT-SWAP`.

For every action sample, the check used:

1. The official announcement ratio.
2. The last active official 1m close before the action and first active official 1m open after it.
3. The official higher-timeframe raw boundary candle.
4. The local materialized backward-adjusted boundary candle.
5. The adjusted previous close to boundary open gap.
6. An idempotent backfill rerun.

Zero-volume halt placeholders are excluded when rebuilding a boundary rollup, matching Binance's official higher-timeframe candle behavior.

## OKX Results

| Symbol | Official ratio | Boundary (UTC) | Observed 1m ratio | Checked TF | Official raw boundary OHLC | Local adjusted boundary OHLC | Adjusted gap | Result |
| --- | ---: | --- | ---: | --- | --- | --- | ---: | --- |
| OPENAI-USDT-SWAP | 10 | 2026-06-30 07:00 | 10.00263004 | 1H | 136.88 / 149.90 / 131.40 / 134.29 | 136.88 / 149.90 / 131.40 / 134.29 | -0.03% | PASS |
| ANTHROPIC-USDT-SWAP | 10 | 2026-06-30 08:06 | 10.00023320 | 1H | 1703.10 / 1719.87 / 167.64 / 173.52 | 170.31 / 174.25 / 167.64 / 173.52 | 0.00% | PASS |
| SPCX-USDT-SWAP | 12.52 | 2026-06-02 07:10 | 12.51753009 | 2H | 2400.10 / 2410.20 / 190.25 / 197.26 | 191.701278 / 203.51 / 189.696486 / 197.26 | +0.0042% | PASS |

Notes:

- OPENAI's boundary is aligned to the 1H bucket, so the official raw and adjusted 1H values are identical.
- ANTHROPIC's 08:00 1H candle spans the 08:06 action. The raw candle is mixed-scale; the adjusted candle is rebuilt from adjusted 1m bars.
- SPACEX was renamed to SPCX. The announcement predecessor is retained as evidence, while factors and history use the successor `SPCX-USDT-SWAP`.

## Binance Results

| Symbol | Official ratio | Boundary (UTC) | Observed 1m ratio | Checked TF | Official raw boundary OHLC | Local adjusted boundary OHLC | Adjusted gap | Result |
| --- | ---: | --- | ---: | --- | --- | --- | ---: | --- |
| SPCXUSDT | 1.1 | 2026-06-10 09:10 | 1.09282460 | 2H | 173.46 / 174.33 / 154.83 / 160.35 | 157.690909 / 162.00 / 154.83 / 160.35 | 0.00% | PASS |
| CRWDUSDT | 4 | 2026-07-02 13:35 | 4.00673750 | 1H | 192.95 / 206.00 / 184.95 / 199.76 | 192.95 / 206.00 / 184.95 / 199.76 | -0.17% | PASS |
| KORUUSDT | 20 | 2026-07-15 09:35 | 21.21296296 | 15m | 22.68 / 25.20 / 22.68 / 23.71 | 22.68 / 25.20 / 22.68 / 23.71 | -5.72% | PASS with note |

Notes:

- Binance emits zero-volume old-scale 1m placeholders during some halt windows. Its official higher-timeframe candles omit those placeholders. Boundary rebuilding now follows that behavior.
- KORU's official ratio is 20, while the active pre/post market prices imply 21.21296296. The factor intentionally uses the official ratio, leaving a real `-5.72%` repricing gap. Replacing the official factor with the observed gap would be incorrect.

## Control And Persistence Checks

- `ZHIPU-USDT-SWAP`: no rebase/split announcement was found in the scanned range. Recent adjusted requests return raw prices with `adjustment_status=not_required`.
- All six action symbols return `adjustment_status=adjusted` with the exchange-specific official-announcement provider.
- The raw evidence fields of every checked boundary candle match the exchange's official higher-timeframe REST response.
- Re-running the backfill returns `SKIP existing` for all six actions and re-repairs/materializes the boundary rows without duplicating factors.
- `go test ./...`, `go vet ./...`, and `git diff --check` passed locally. Relevant package tests also passed on `ticket`.
- `crypto-ticket.service` remained active after deployment and served live adjusted queries.

## Coverage And Remaining Limits

The implementation covers:

- Official historical announcement pagination for Binance and OKX.
- Official-ratio factor backfill and official 1m boundary detection.
- Rename + rebase where OKX serves predecessor history under the successor symbol.
- Raw boundary repair and adjusted boundary materialization from 1m through 1D.
- Partial materialized/dynamic result merging.
- Explicit OKX no-action coverage.

Remaining limits:

- Crossing boundary candles above 1D (`2D`, weekly, monthly) are not materialized yet. Dynamic close-boundary factor selection alone cannot fully repair their mixed-scale OHLC.
- No-action coverage is only written for explicitly requested OKX symbols and scan ranges. Unknown or unscanned symbols correctly remain `missing_factor`.
- Announcement parsers depend on public exchange formats and should be regression-tested when Binance CMS or OKX Help changes its schema.

## Conclusion

The tested Binance and OKX action types are covered correctly for 1m through 1D serving paths. Official raw boundary evidence is preserved, adjusted boundary candles are continuous except for legitimate market repricing, rename handling works, and backfill is idempotent. The primary remaining functional gap is materialization of crossing candles above 1D.
