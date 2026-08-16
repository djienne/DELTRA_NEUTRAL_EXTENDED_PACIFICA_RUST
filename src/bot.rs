/// Funding rate arbitrage bot orchestration and state management
use crate::{
    OpportunityFinder, RestClient, PacificaTrading, PacificaCredentials, Position,
    trading::{
        backoff_delay_ms, calculate_position_size, close_delta_neutral_position,
        looks_like_rate_limit, open_delta_neutral_position, DeltaNeutralPosition,
    },
    OpportunityConfig,
};
use crate::pacifica::types::PacificaPosition;
use crate::pacifica::PacificaWsTrading;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;
use futures_util::FutureExt;
use tracing::{info, warn, error};
use prettytable::{Table, Row, Cell, format};
use colored::*;

/// Default state path.
///
/// This MUST match the path docker-compose mounts and sets via STATE_FILE_PATH
/// (`./state` -> `/app/state`, `STATE_FILE_PATH=/app/state/bot_state.json`).
///
/// It used to be the bare `bot_state.json` at the repo root, which meant the container
/// wrote `state/bot_state.json` while any host-run binary (notably `emergency_exit`)
/// read a *different, stale* file at the root. That is how emergency_exit came to report
/// "EMERGENCY EXIT SUCCESSFUL / All positions closed" while both legs were still open:
/// it loaded a stale root file, found `current_position: None`, and returned Ok(())
/// without ever querying the exchanges.
const DEFAULT_STATE_FILE: &str = "state/bot_state.json";

/// Pre-collapse location, kept only so we can warn if it is still lying around.
const LEGACY_STATE_FILE: &str = "bot_state.json";
const MONITORING_INTERVAL_MINUTES: u64 = 15;
const LIVE_POSITIONS_MAX_ATTEMPTS: u32 = 6;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotState {
    pub current_position: Option<DeltaNeutralPosition>,
    pub last_rotation_time: Option<u64>,
    pub total_rotations: u64,
}

impl BotState {
    pub fn new() -> Self {
        Self {
            current_position: None,
            last_rotation_time: None,
            total_rotations: 0,
        }
    }

    /// Load state from JSON file
    pub fn load_from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        if Path::new(path).exists() {
            let content = fs::read_to_string(path)?;
            match serde_json::from_str::<BotState>(&content) {
                Ok(state) => {
                    info!("Loaded bot state from {}: {} rotations, position: {}",
                        path,
                        state.total_rotations,
                        if state.current_position.is_some() { "active" } else { "none" }
                    );
                    Ok(state)
                }
                Err(e) => {
                    warn!("Failed to parse state file {}: {}. Trying backup.", path, e);
                    let backup_path = format!("{}.bak", path);
                    if Path::new(&backup_path).exists() {
                        let backup_content = fs::read_to_string(&backup_path)?;
                        let backup_state: BotState = serde_json::from_str(&backup_content)?;
                        info!("Loaded bot state from backup {}", backup_path);
                        Ok(backup_state)
                    } else {
                        warn!("No valid backup found. Starting fresh.");
                        let state = Self::new();
                        if let Err(write_err) = state.save_to_file(path) {
                            warn!("Failed to write initial state file {}: {}", path, write_err);
                        }
                        Ok(state)
                    }
                }
            }
        } else {
            info!("No existing state file found, starting fresh");
            let state = Self::new();
            if let Err(e) = state.save_to_file(path) {
                warn!("Failed to write initial state file {}: {}", path, e);
            }
            Ok(state)
        }
    }

    /// Save state to JSON file
    pub fn save_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        if Path::new(path).exists() {
            let backup_path = format!("{}.bak", path);
            if let Err(e) = fs::copy(path, &backup_path) {
                warn!("Failed to write backup state file {}: {}", backup_path, e);
            }
        }
        let content = serde_json::to_string_pretty(self)?;
        let temp_path = format!("{}.tmp", path);
        fs::write(&temp_path, &content)?;
        if let Err(e) = fs::rename(&temp_path, path) {
            warn!("Atomic state file replace failed: {}. Falling back to direct write.", e);
            fs::write(path, content)?;
            let _ = fs::remove_file(&temp_path);
        }
        info!("Saved bot state to {}", path);
        Ok(())
    }

    /// Check if current position should be rotated based on configured hold time
    pub fn should_rotate(&self, hold_time_hours: u64) -> bool {
        if let Some(pos) = &self.current_position {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let elapsed_hours = (now - pos.opened_at) / 3600;
            elapsed_hours >= hold_time_hours
        } else {
            false
        }
    }

    /// Get time remaining until rotation (in hours)
    pub fn hours_until_rotation(&self, hold_time_hours: u64) -> Option<f64> {
        if let Some(pos) = &self.current_position {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let elapsed_hours = (now - pos.opened_at) as f64 / 3600.0;
            let remaining = hold_time_hours as f64 - elapsed_hours;
            Some(remaining.max(0.0))
        } else {
            None
        }
    }
}

enum RecoveryOutcome {
    NoAction,
    Recovered,
    Blocked(String),
}

pub struct FundingBot {
    extended_client: RestClient,
    pacifica_client: PacificaTrading,
    pacifica_creds: PacificaCredentials,
    opportunity_finder: OpportunityFinder,
    config: OpportunityConfig,
    state: BotState,
    state_path: String,
    stark_private_key: String,
    stark_public_key: String,
    vault_id: String,
}

fn resolve_state_path() -> String {
    let path = std::env::var("STATE_FILE_PATH").unwrap_or_else(|_| DEFAULT_STATE_FILE.to_string());

    // A leftover root-level bot_state.json is a decoy: it is the file the old default
    // pointed at, so anything still reading it sees stale positions. Warn loudly rather
    // than silently picking one, because "which file am I reading" is exactly the
    // ambiguity that made emergency_exit report success over an open position.
    if path != LEGACY_STATE_FILE && Path::new(LEGACY_STATE_FILE).exists() {
        warn!(
            "Stale legacy state file '{}' exists but '{}' is in use. \
             The legacy file is NOT read - delete it once you have confirmed it holds \
             nothing you need, so it cannot be mistaken for live state.",
            LEGACY_STATE_FILE, path
        );
    }

    path
}

impl FundingBot {
    pub fn new(
        extended_api_key: Option<String>,
        pacifica_creds: PacificaCredentials,
        config: OpportunityConfig,
        stark_private_key: String,
        stark_public_key: String,
        vault_id: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let extended_client = RestClient::new_mainnet(extended_api_key.clone())?;
        let pacifica_client = PacificaTrading::new(pacifica_creds.clone());
        let opportunity_finder = OpportunityFinder::new(
            extended_api_key.clone(),
            pacifica_creds.clone(),
            config.clone(),
        )?;

        let state_path = resolve_state_path();
        let state = BotState::load_from_file(&state_path)?;

        Ok(Self {
            extended_client,
            pacifica_client,
            pacifica_creds,
            opportunity_finder,
            config,
            state,
            state_path,
            stark_private_key,
            stark_public_key,
            vault_id,
        })
    }

    async fn fetch_live_positions_with_backoff(
        &self,
    ) -> Result<(Vec<Position>, Vec<PacificaPosition>), Box<dyn std::error::Error>> {
        let mut attempt = 0;

        loop {
            attempt += 1;

            let extended_positions = self.extended_client.get_positions(None).await;
            let pacifica_positions = self.pacifica_client.get_positions().await;

            match (extended_positions, pacifica_positions) {
                (Ok(ext), Ok(pac)) => return Ok((ext, pac)),
                (ext_res, pac_res) => {
                    let mut parts = Vec::new();
                    if let Err(e) = &ext_res {
                        parts.push(format!("Extended: {}", e));
                    }
                    if let Err(e) = &pac_res {
                        parts.push(format!("Pacifica: {}", e));
                    }
                    let err_msg = if parts.is_empty() {
                        "Unknown error".to_string()
                    } else {
                        parts.join(" | ")
                    };

                    if attempt >= LIVE_POSITIONS_MAX_ATTEMPTS {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            err_msg,
                        )));
                    }

                    let rate_limited = looks_like_rate_limit(&err_msg);
                    let delay_ms = backoff_delay_ms(attempt, rate_limited);
                    warn!(
                        "Failed to fetch live positions (attempt {}/{}{}): {}. Retrying in {}ms...",
                        attempt,
                        LIVE_POSITIONS_MAX_ATTEMPTS,
                        if rate_limited { " - rate limited" } else { "" },
                        err_msg,
                        delay_ms
                    );
                    sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }

    async fn recover_state_if_untracked(
        &mut self,
    ) -> Result<RecoveryOutcome, Box<dyn std::error::Error>> {
        if self.state.current_position.is_some() {
            return Ok(RecoveryOutcome::NoAction);
        }

        let (extended_positions, pacifica_positions) = self.fetch_live_positions_with_backoff().await?;

        if extended_positions.is_empty() && pacifica_positions.is_empty() {
            return Ok(RecoveryOutcome::NoAction);
        }

        let mut extended_symbols = HashSet::new();
        for pos in &extended_positions {
            let sym = pos.market.strip_suffix("-USD").unwrap_or(&pos.market).to_string();
            extended_symbols.insert(sym);
        }

        let mut pacifica_symbols = HashSet::new();
        for pos in &pacifica_positions {
            pacifica_symbols.insert(pos.symbol.clone());
        }

        let mut ext_list: Vec<String> = extended_symbols.iter().cloned().collect();
        ext_list.sort();
        let mut pac_list: Vec<String> = pacifica_symbols.iter().cloned().collect();
        pac_list.sort();
        let mut overlap: Vec<String> = extended_symbols
            .intersection(&pacifica_symbols)
            .cloned()
            .collect();
        overlap.sort();

        let details = format!(
            "extended: {} position(s) [{}]; pacifica: {} position(s) [{}]; overlap: [{}]",
            extended_positions.len(),
            ext_list.join(", "),
            pacifica_positions.len(),
            pac_list.join(", "),
            overlap.join(", ")
        );

        let mut all_symbols: Vec<String> = extended_symbols.union(&pacifica_symbols).cloned().collect();
        all_symbols.sort();

        let symbol = if overlap.len() == 1 {
            overlap[0].clone()
        } else if overlap.is_empty() && all_symbols.len() == 1 {
            all_symbols[0].clone()
        } else {
            return Ok(RecoveryOutcome::Blocked(details));
        };

        let extended_position = extended_positions
            .into_iter()
            .find(|p| p.market.strip_suffix("-USD").unwrap_or(&p.market) == symbol);
        let pacifica_position = pacifica_positions
            .into_iter()
            .find(|p| p.symbol == symbol);

        let opened_at = pacifica_position
            .as_ref()
            .and_then(|pos| if pos.created_at > 0 { Some((pos.created_at / 1000) as u64) } else { None })
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            });

        let mut target_notional_usd: f64 = 0.0;
        if let Some(ref ext_pos) = extended_position {
            let value = ext_pos.value_f64();
            if value > 0.0 {
                target_notional_usd = target_notional_usd.max(value);
            }
        }
        if let Some(ref pac_pos) = pacifica_position {
            let value = pac_pos.size() * pac_pos.entry();
            if value > 0.0 {
                target_notional_usd = target_notional_usd.max(value);
            }
        }

        let position = DeltaNeutralPosition {
            symbol,
            extended_position,
            pacifica_position,
            opened_at,
            target_notional_usd,
        };

        self.state.current_position = Some(position);
        if self.state.last_rotation_time.is_none() {
            self.state.last_rotation_time = Some(opened_at);
        }
        self.state.save_to_file(&self.state_path)?;
        info!("Recovered bot state from live positions. {}", details);

        Ok(RecoveryOutcome::Recovered)
    }

    /// Reconcile saved state with live exchange positions.
    /// If saved state indicates an active position but neither exchange has it,
    /// clear the state to avoid erroneous closes/rotations. If only one leg exists,
    /// keep that leg in state so a subsequent close will only act on the live leg.
    ///
    /// Returns Ok(()) if reconciliation was successful (or state was empty).
    /// Returns Err if network/API calls failed - in this case state is NOT modified.
    pub async fn reconcile_state(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let Some(saved_pos) = self.state.current_position.clone() else {
            // Nothing to reconcile
            return Ok(());
        };

        let symbol = saved_pos.symbol.clone();
        let extended_market = format!("{}-USD", symbol);

        // Query live positions on Extended
        // We propagate errors here so we don't accidentally clear state on network failure
        let live_ext_list = match self.extended_client.get_positions(Some(&extended_market)).await {
            Ok(list) => list,
            Err(e) => {
                warn!("Network error checking Extended positions: {}. Keeping existing state.", e);
                return Err(e.into());
            }
        };
        let live_ext = live_ext_list.into_iter().find(|p| p.market == extended_market);

        // Query live position on Pacifica
        let live_pac = match self.pacifica_client.get_position(&symbol).await {
            Ok(pos_opt) => pos_opt,
            Err(e) => {
                warn!("Network error checking Pacifica positions: {}. Keeping existing state.", e);
                return Err(e.into());
            }
        };

        // If both legs are missing, clear state
        if live_ext.is_none() && live_pac.is_none() {
            warn!(
                "State shows active {}, but no live positions found on either exchange. Clearing stale state.",
                symbol
            );
            self.state.current_position = None;
            self.state.save_to_file(&self.state_path)?;
            return Ok(());
        }

        // Otherwise, update state to reflect only the legs that actually exist
        let mut updated = saved_pos.clone();
        updated.extended_position = live_ext;
        updated.pacifica_position = live_pac;
        self.state.current_position = Some(updated);
        self.state.save_to_file(&self.state_path)?;
        Ok(())
    }

    /// Display current status summary
    pub async fn display_status(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_BOX_CHARS);

        // Title Row
        table.set_titles(Row::new(vec![
            Cell::new("FUNDING RATE BOT STATUS")
                .style_spec("cb") // Center, Bold
                .with_hspan(2)
        ]));

        if let Some(pos) = &self.state.current_position {
            let hold_time_hours = self.config.trading.hold_time_hours;
            let hours_remaining = self.state.hours_until_rotation(hold_time_hours).unwrap_or(0.0);

            // Convert opened_at timestamp to datetime
            let opened_datetime = chrono::DateTime::from_timestamp(pos.opened_at as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "Unknown".to_string());

            // Calculate rotation time
            let rotation_timestamp = pos.opened_at + (hold_time_hours * 3600);
            let rotation_datetime = chrono::DateTime::from_timestamp(rotation_timestamp as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "Unknown".to_string());

            let hours_formatted = format!("{:.1} hours", hours_remaining);

            table.add_row(Row::new(vec![Cell::new("Symbol"), Cell::new(&pos.symbol).style_spec("b")]));
            table.add_row(Row::new(vec![Cell::new("Notional"), Cell::new(&format!("${:.2}", pos.target_notional_usd))]));
            table.add_row(Row::new(vec![Cell::new("Opened"), Cell::new(&opened_datetime)]));
            table.add_row(Row::new(vec![Cell::new("Rotation"), Cell::new(&rotation_datetime)]));
            table.add_row(Row::new(vec![Cell::new("Time Remaining"), Cell::new(&hours_formatted).style_spec(if hours_remaining < 1.0 { "Fr" } else { "Fg" })])); // Red if < 1h, else Green

            // Extended Position Status
            let ext_status = if pos.extended_position.is_some() {
                "ACTIVE".green().bold()
            } else {
                "NONE".red().bold()
            };
            table.add_row(Row::new(vec![Cell::new("Extended Position"), Cell::new(&ext_status.to_string())]));

            // Pacifica Position Status
            let pac_status = if pos.pacifica_position.is_some() {
                "ACTIVE".green().bold()
            } else {
                "NONE".red().bold()
            };
            table.add_row(Row::new(vec![Cell::new("Pacifica Position"), Cell::new(&pac_status.to_string())]));

            // Fetch current positions for PnL display
            if let Ok(extended_positions) = self.extended_client.get_positions(None).await {
                if let Some(ext_pos) = extended_positions.iter().find(|p| p.market.starts_with(&pos.symbol)) {
                    let pnl = ext_pos.unrealized_pnl.as_ref()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    let pnl_formatted = format!("${:.2}", pnl);
                    let style = if pnl >= 0.0 { "Fg" } else { "Fr" };
                    table.add_row(Row::new(vec![Cell::new("Extended PnL"), Cell::new(&pnl_formatted).style_spec(style)]));
                }
            }

            if let Ok(pacifica_positions) = self.pacifica_client.get_positions().await {
                if let Some(pac_pos) = pacifica_positions.iter().find(|p| p.symbol == pos.symbol) {
                    let entry = pac_pos.entry();
                    let size = pac_pos.size();
                    table.add_row(Row::new(vec![Cell::new("Pacifica Entry"), Cell::new(&format!("${:.2}", entry))]));
                    table.add_row(Row::new(vec![Cell::new("Pacifica Size"), Cell::new(&format!("{:.6}", size))]));
                }
            }
        } else {
            table.add_row(Row::new(vec![
                Cell::new("Status"),
                Cell::new("NO ACTIVE POSITION").style_spec("Fy") // Yellow
            ]));
        }

        table.add_row(Row::new(vec![Cell::new("Total Rotations"), Cell::new(&self.state.total_rotations.to_string())]));

        table.printstd();

        Ok(())
    }

    /// Find and open the best opportunity
    pub async fn open_best_opportunity(
        &mut self,
        extended_api_key: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("{}", "🔍 Scanning for best opportunity...");

        // Safety net: if state is empty but exchanges report open positions, abort opening
        match self.recover_state_if_untracked().await? {
            RecoveryOutcome::Recovered => {
                warn!("Recovered bot state from live positions; skipping new open.");
                return Ok(());
            }
            RecoveryOutcome::Blocked(details) => {
                return Err(format!(
                    "Live positions exist while bot state is empty. Aborting open to prevent duplicates. {}",
                    details
                ).into());
            }
            RecoveryOutcome::NoAction => {}
        }

        let scan_result = self.opportunity_finder.scan(extended_api_key.clone()).await?;

        // Display comprehensive scan summary
        scan_result.display_summary(&self.config.filters);

        if scan_result.opportunities.is_empty() {
            warn!("{}", "No opportunities found matching criteria");
            return Ok(());
        }

        let best = &scan_result.opportunities[0];
        info!("{} {} {}",
            "✅ Selected best opportunity:",
            best.symbol,
            format!("(Net APR: {:.2}%)", best.best_net_apr));
        info!("   {}: {}", "Strategy", best.best_direction);
        info!("   {}: ${:.0}", "Volume", best.total_volume_24h);
        info!("   {}: Ext {:.3}%, Pac {:.3}%, Cross {:.3}%",
            "Spreads",
            best.extended_spread_pct, best.pacifica_spread_pct, best.cross_spread_pct);

        // Determine position direction
        let long_on_extended = best.best_direction.contains("Long Extended");

        // Get market symbols
        let extended_market = format!("{}-USD", best.symbol);
        let pacifica_market = best.symbol.clone();

        // Fetch current prices and account info
        let extended_balance = self.extended_client.get_balance().await?;
        let extended_free = extended_balance.available_for_trade.parse::<f64>()?;

        // Fetch Pacifica account balance via WebSocket
        let pacifica_ws = PacificaWsTrading::new(self.pacifica_creds.clone(), false); // false = mainnet
        let pacifica_account_info = pacifica_ws.get_account_info().await?;
        let pacifica_free = pacifica_account_info.available_to_spend_f64();

        info!("{} {}", "💰 Extended free collateral:", format!("${:.2}", extended_free));
        info!("{} {}", "💰 Pacifica free collateral:", format!("${:.2}", pacifica_free));

        // Get lot sizes
        let extended_market_config = self.extended_client.get_market_config(&extended_market).await?;
        let extended_lot_size = extended_market_config.trading_config.min_order_size_change.parse::<f64>()?;

        let pacifica_markets = self.pacifica_client.get_market_info().await?;
        let pacifica_market_info = pacifica_markets.get(&pacifica_market)
            .ok_or_else(|| format!("Pacifica market {} not found", pacifica_market))?;
        let pacifica_lot_size = pacifica_market_info.lot_size.parse::<f64>()?;

        // Get current price
        let orderbook = self.extended_client.get_orderbook(&extended_market).await?;
        let current_price = if let (Some(bid), Some(ask)) = (orderbook.bid.first(), orderbook.ask.first()) {
            let bid_price = bid.price.parse::<f64>()?;
            let ask_price = ask.price.parse::<f64>()?;
            (bid_price + ask_price) / 2.0
        } else {
            return Err("No orderbook data available".into());
        };

        // Calculate position size
        let position_size = calculate_position_size(
            extended_free,
            pacifica_free,
            extended_lot_size,
            pacifica_lot_size,
            current_price,
            self.config.trading.max_position_size_usd,
        );

        if position_size <= 0.0 {
            return Err("Insufficient capital to open position".into());
        }

        info!("{} {:.6} {} ({})",
            "📊 Calculated position size:",
            position_size,
            best.symbol,
            format!("${:.2}", position_size * current_price));

        // Set leverage to 1x on both exchanges before opening position
        info!("{}", "⚙️  Setting leverage to 1x on both exchanges...");

        // Set leverage on BOTH venues. A failure here ABORTS the open.
        //
        // These used to log "continuing anyway" and proceed. That is unsafe: the
        // bot deploys 95% of available capital believing it is at 1x. If the venue
        // still carries a previously-set leverage (say 20x isolated), the same
        // notional posts a twentieth of the margin, and a ~6% adverse move over the
        // 23h hold liquidates that leg -- leaving the other side naked and
        // directional, from a position the operator believed was market-neutral at
        // 1x.
        //
        // Worse, the failure is asymmetric: setting 1x on one venue and silently
        // leaving the other at 20x produces legs that are equal in SIZE but wildly
        // unequal in margin consumption, which is exactly the blow-up mode
        // cross-exchange delta-neutral is supposed to avoid.
        self.extended_client
            .update_leverage(&extended_market, "1")
            .await
            .map_err(|e| format!(
                "Refusing to open {}: could not set Extended leverage to 1x ({}). \
                 Opening at unknown leverage risks liquidating one leg.",
                extended_market, e
            ))?;
        info!("   ✅ Extended leverage set to 1x for {}", extended_market);

        self.pacifica_client
            .update_leverage(&pacifica_market, 1)
            .await
            .map_err(|e| format!(
                "Refusing to open {}: could not set Pacifica leverage to 1x ({}). \
                 Extended is already at 1x; opening now would give the two legs \
                 mismatched margin profiles.",
                pacifica_market, e
            ))?;
        info!("   ✅ Pacifica leverage set to 1x for {}", pacifica_market);

        // Open delta neutral position
        let position = open_delta_neutral_position(
            &best.symbol,
            long_on_extended,
            position_size,
            current_price,
            &self.extended_client,
            &mut self.pacifica_client,
            &extended_market,
            &pacifica_market,
            &self.stark_private_key,
            &self.stark_public_key,
            &self.vault_id,
        ).await.map_err(|e| format!("Failed to open position: {}", e))?;

        // Update state
        self.state.current_position = Some(position);
        self.state.last_rotation_time = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
        );
        self.state.total_rotations += 1;
        self.state.save_to_file(&self.state_path)?;

        info!("{}", "✅ Position opened successfully!");

        Ok(())
    }

    /// Close the current position
    pub async fn close_current_position(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.state.current_position.is_some() {
            // Ensure state matches live positions before attempting close
            self.reconcile_state().await.ok();

            // If reconciliation cleared the state, nothing to do
            if self.state.current_position.is_none() {
                warn!("No live positions to close after reconciliation");
                return Ok(());
            }

            let pos = self.state.current_position.as_ref().unwrap();
            info!("{} {}", "🔄 Closing current position:", pos.symbol);

            close_delta_neutral_position(
                pos,
                &self.extended_client,
                &mut self.pacifica_client,
                &self.stark_private_key,
                &self.stark_public_key,
                &self.vault_id,
            ).await.map_err(|e| format!("Failed to close position: {}", e))?;

            // Clear position from state
            self.state.current_position = None;
            self.state.save_to_file(&self.state_path)?;

            info!("{}", "✅ Position closed successfully!");
        } else {
            // DO NOT just return Ok(()) here.
            //
            // This branch used to log "No active position to close" and succeed,
            // which is how emergency_exit printed "EMERGENCY EXIT SUCCESSFUL / All
            // positions closed" while both legs were still open. An empty state
            // file is not evidence of a flat account -- especially for
            // emergency_exit, which historically ran on the host against a
            // different (stale) state path than the container writes.
            //
            // Query BOTH venues account-wide before concluding there is nothing to
            // close. reconcile_state() cannot help: it returns early when
            // current_position is None, so it only ever validates a symbol we
            // already know about.
            warn!("No position in state - querying BOTH venues account-wide before \
                   concluding there is nothing to close");

            let ext_live = self.extended_client.get_positions(None).await
                .map_err(|e| format!("Extended position scan failed: {}. Refusing to \
                                      report success over an unknown account state.", e))?;
            let pac_live = self.pacifica_client.get_positions().await
                .map_err(|e| format!("Pacifica position scan failed: {}. Refusing to \
                                      report success over an unknown account state.", e))?;

            let ext_open: Vec<_> = ext_live.into_iter()
                .filter(|p| p.size.parse::<f64>().unwrap_or(0.0).abs() > 0.0)
                .collect();
            let pac_open: Vec<_> = pac_live.into_iter()
                .filter(|p| p.amount.parse::<f64>().unwrap_or(0.0).abs() > 0.0)
                .collect();

            if ext_open.is_empty() && pac_open.is_empty() {
                info!("Verified flat: no open positions on Extended or Pacifica");
                return Ok(());
            }

            error!(
                "UNTRACKED POSITIONS FOUND: {} on Extended, {} on Pacifica, none in \
                 state. These are unhedged and unmanaged.",
                ext_open.len(), pac_open.len()
            );
            for p in &ext_open {
                error!("  Extended: {} size={} ", p.market, p.size);
            }
            for p in &pac_open {
                error!("  Pacifica: {} amount={} side={}", p.symbol, p.amount, p.side);
            }
            return Err(format!(
                "Refusing to report success: {} Extended and {} Pacifica position(s) \
                 are open but absent from state. Close them manually and verify both \
                 venues before restarting.",
                ext_open.len(), pac_open.len()
            ).into());
        }

        Ok(())
    }

    /// Check if the current position is imbalanced (only one leg active)
    pub fn is_imbalanced(&self) -> bool {
        if let Some(pos) = &self.state.current_position {
            // Imbalanced if one is Some and the other is None
            // (If both are None, reconcile_state sets current_position to None, so we wouldn't be here)
            // (If both are Some, it's healthy)
            pos.extended_position.is_some() ^ pos.pacifica_position.is_some()
        } else {
            false
        }
    }

    /// Main bot loop
    pub async fn run(&mut self, extended_api_key: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
        info!("{}", "🚀 Starting Funding Rate Arbitrage Bot");
        info!("{} {} {}",
            "📊 Monitoring interval:",
            MONITORING_INTERVAL_MINUTES,
            "minutes");
        info!("{} {} {}",
            "⏱️  Position hold time:",
            self.config.trading.hold_time_hours,
            "hours");
        info!("{}", "🛑 Press Ctrl+C to stop gracefully");

        loop {
            // Non-blocking check for Ctrl+C (gracefully exit; keep positions open)
            if tokio::signal::ctrl_c().now_or_never().is_some() {
                info!("{}", "");
                info!("{}", "🛑 Shutdown signal received. Stopping bot gracefully...");
                info!("{}", "ℹ️  Open positions (if any) will remain open.");
                info!("{}", "   Manage them from the exchange dashboards or restart the bot.");
                info!("{}", "👋 Bot stopped. Goodbye!");
                return Ok(());
            }

            // Reconcile any stale state before acting
            if let Err(e) = self.reconcile_state().await {
                warn!("Network error during state reconciliation: {}. Skipping cycle to prevent unsafe actions.", e);
                sleep(Duration::from_secs(60)).await; // Wait 1 minute before retrying
                continue;
            }

            // CRITICAL: Check for imbalance immediately after reconciliation
            if self.is_imbalanced() {
                error!("{}", "⚠️  CRITICAL: Position imbalance detected! One leg is missing.");
                info!("{}", "🚨 Initiating EMERGENCY CLOSE of remaining leg to preserve capital...");
                
                if let Err(e) = self.close_current_position().await {
                    error!("{} {}", "❌ Failed to close imbalanced position:", e);
                    info!("{}", "Will retry immediately...");
                    // Don't sleep long if we are in a critical state
                    sleep(Duration::from_secs(5)).await;
                    continue;
                } else {
                    info!("{}", "✅ Emergency close successful. State is now clean.");
                    // Continue to normal loop to potentially re-open if opportunity exists
                }
            }

            // If state is empty but live positions exist, refuse to open to prevent duplicates
            if self.state.current_position.is_none() {
                match self.recover_state_if_untracked().await {
                    Ok(RecoveryOutcome::Recovered) => {
                        info!("Recovered bot state from live positions. Monitoring only.");
                    }
                    Ok(RecoveryOutcome::Blocked(details)) => {
                        error!("⚠️  Live positions detected while bot state is empty. Skipping open/rotation to avoid duplicate exposure. {}", details);
                        info!("Resolve by closing manually (or run the emergency_exit binary) or reconstruct bot_state.json, then restart.");
                        sleep(Duration::from_secs(MONITORING_INTERVAL_MINUTES * 60)).await;
                        continue;
                    }
                    Err(e) => {
                        warn!("Could not verify live positions (skipping cycle to avoid duplicates): {}", e);
                        sleep(Duration::from_secs(60)).await;
                        continue;
                    }
                    Ok(RecoveryOutcome::NoAction) => {}
                }
            }

            // Display status
            self.display_status().await?;

            // Always scan and display opportunities at start of each cycle
            info!("");
            info!("{}", "🔍 Scanning current market opportunities...");
            if let Ok(scan_result) = self.opportunity_finder.scan(extended_api_key.clone()).await {
                scan_result.display_summary(&self.config.filters);
            } else {
                warn!("{}", "Failed to scan opportunities");
            }
            info!("");

            // Check if we need to rotate
            if self.state.should_rotate(self.config.trading.hold_time_hours) {
                info!("{} {} {}",
                    "⏰ Position has been open for",
                    self.config.trading.hold_time_hours,
                    "hours, rotating...");

                // Close current position
                if let Err(e) = self.close_current_position().await {
                    error!("{} {}", "Failed to close position:", e);
                    info!("{}", "Will retry next cycle.");
                    sleep(Duration::from_secs(MONITORING_INTERVAL_MINUTES * 60)).await;
                    continue;
                }

                // Wait a bit before opening new position
                sleep(Duration::from_secs(5)).await;

                // Open new position
                if let Err(e) = self.open_best_opportunity(extended_api_key.clone()).await {
                    error!("{} {}", "Failed to open new position:", e);
                    info!("{}", "Will retry next cycle.");
                }
            } else if self.state.current_position.is_none() {
                // No position, try to open one
                info!("{}", "📭 No active position, looking for opportunity...");

                if let Err(e) = self.open_best_opportunity(extended_api_key.clone()).await {
                    error!("{} {}", "Failed to open position:", e);
                    info!("{}", "Will retry next cycle.");
                }
            } else {
                // Position active, just monitoring
                if let Some(hours) = self.state.hours_until_rotation(self.config.trading.hold_time_hours) {
                    info!("{} {} {}",
                        "⏳ Position active,",
                        format!("{:.1}", hours),
                        "hours until rotation");
                }
            }

            // Wait for next monitoring cycle (interruptible by Ctrl+C)
            info!("{} {} {}",
                "😴 Sleeping for",
                MONITORING_INTERVAL_MINUTES,
                "minutes...");
            let wait = sleep(Duration::from_secs(MONITORING_INTERVAL_MINUTES * 60));
            tokio::pin!(wait);
            tokio::select! {
                _ = &mut wait => {},
                _ = tokio::signal::ctrl_c() => {
                    info!("{}", "");
                    info!("{}", "🛑 Shutdown signal received during sleep. Stopping gracefully.");
                    info!("{}", "ℹ️  Open positions (if any) will remain open.");
                    return Ok(());
                }
            }
        }
    }
}
