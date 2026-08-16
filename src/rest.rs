use crate::error::{ConnectorError, Result};
use crate::types::{
    AccountInfo, ApiResponse, Balance, BidAsk, FeeInfo, FundingRateData, FundingRateInfo, MarketConfig,
    MarketInfo, OrderBook, OrderRequest, OrderResponse, OrderSide, OrderType, PaginatedResponse,
    Position, Settlement, TimeInForce,
};
use reqwest::Client;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// REST API client for Extended exchange
pub struct RestClient {
    client: Client,
    base_url: String,
    api_key: Option<String>,
}

impl RestClient {
    /// Create a new REST client for mainnet
    pub fn new_mainnet(api_key: Option<String>) -> Result<Self> {
        Self::new("https://api.starknet.extended.exchange/api/v1", api_key)
    }

    /// Create a new REST client for testnet
    pub fn new_testnet(api_key: Option<String>) -> Result<Self> {
        Self::new(
            "https://api.starknet.sepolia.extended.exchange/api/v1",
            api_key,
        )
    }

    /// Create a new REST client with custom base URL
    pub fn new(base_url: &str, api_key: Option<String>) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("extended-connector/0.1.0")
            .build()?;

        Ok(Self {
            client,
            base_url: base_url.to_string(),
            api_key,
        })
    }

    /// Get orderbook for a specific market
    pub async fn get_orderbook(&self, market: &str) -> Result<OrderBook> {
        let url = format!("{}/info/markets/{}/orderbook", self.base_url, market);
        debug!("Fetching orderbook for {} from {}", market, url);

        let mut request = self.client.get(&url);

        // Add API key if provided (though not needed for public endpoints)
        if let Some(api_key) = &self.api_key {
            request = request.header("X-Api-Key", api_key);
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            error!("API error: {} - {}", status, error_text);
            return Err(ConnectorError::ApiError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let api_response: ApiResponse<OrderBook> = response.json().await?;

        match api_response.data {
            Some(orderbook) => {
                info!(
                    "Fetched orderbook for {} - {} bids, {} asks",
                    market,
                    orderbook.bid.len(),
                    orderbook.ask.len()
                );
                Ok(orderbook)
            }
            None => {
                let error_msg = api_response
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("API error response: {}", error_msg);
                Err(ConnectorError::ApiError(error_msg))
            }
        }
    }

    /// Get best bid/ask for a specific market
    pub async fn get_bid_ask(&self, market: &str) -> Result<BidAsk> {
        let orderbook = self.get_orderbook(market).await?;
        Ok(BidAsk::from(&orderbook))
    }

    /// Get best bid/ask for multiple markets concurrently
    pub async fn get_multiple_bid_asks(&self, markets: &[String]) -> Vec<Result<BidAsk>> {
        let mut tasks = Vec::new();

        for market in markets {
            let market = market.clone();
            let client = self.clone_for_parallel();
            tasks.push(tokio::spawn(async move {
                client.get_bid_ask(&market).await
            }));
        }

        let mut results = Vec::new();
        for task in tasks {
            match task.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(ConnectorError::Other(format!(
                    "Task join error: {}",
                    e
                )))),
            }
        }

        results
    }

    /// Helper to clone client for parallel requests
    fn clone_for_parallel(&self) -> Self {
        Self {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            api_key: self.api_key.clone(),
        }
    }

    /// Get all available markets
    pub async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let url = format!("{}/info/markets", self.base_url);
        debug!("Fetching all markets from {}", url);

        let mut request = self.client.get(&url);

        if let Some(api_key) = &self.api_key {
            request = request.header("X-Api-Key", api_key);
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            error!("API error: {} - {}", status, error_text);
            return Err(ConnectorError::ApiError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let api_response: ApiResponse<Vec<MarketInfo>> = response.json().await?;

        match api_response.data {
            Some(markets) => {
                info!("Fetched {} markets", markets.len());
                Ok(markets)
            }
            None => {
                let error_msg = api_response
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("API error response: {}", error_msg);
                Err(ConnectorError::ApiError(error_msg))
            }
        }
    }

    /// Get latest funding rate for a specific market from market stats endpoint
    /// The fundingRate field represents the current hourly funding rate
    pub async fn get_funding_rate(&self, market: &str) -> Result<Option<FundingRateInfo>> {
        let url = format!("{}/info/markets/{}/stats", self.base_url, market);
        debug!("Fetching market stats for {} from {}", market, url);

        let mut request = self.client.get(&url);

        if let Some(api_key) = &self.api_key {
            request = request.header("X-Api-Key", api_key);
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            warn!("Could not fetch market stats for {}: {} - {}", market, status, error_text);
            return Ok(None);
        }

        #[derive(serde::Deserialize)]
        struct MarketStatsResponse {
            status: String,
            data: Option<MarketStatsData>,
        }

        #[derive(serde::Deserialize)]
        #[allow(dead_code)]
        struct MarketStatsData {
            #[serde(rename = "fundingRate")]
            funding_rate: String,
        }

        let api_response: MarketStatsResponse = response.json().await?;

        match api_response.data {
            Some(data) => {
                let rate: f64 = data.funding_rate.parse().unwrap_or(0.0);
                let rate_percentage = rate * 100.0;
                let is_positive = rate >= 0.0;
                let now = chrono::Utc::now().timestamp_millis() as u64;

                let info = FundingRateInfo {
                    market: market.to_string(),
                    rate,
                    rate_percentage,
                    timestamp: now,
                    is_positive,
                };

                debug!("Fetched funding rate for {} from stats: {}", market, info.rate_percentage);
                Ok(Some(info))
            }
            None => {
                debug!("No market stats data available for {}", market);
                Ok(None)
            }
        }
    }

    /// Get funding rates for all active markets
    pub async fn get_all_funding_rates(&self) -> Result<Vec<FundingRateInfo>> {
        // First, get all markets
        let markets = self.get_all_markets().await?;

        // Filter only active markets
        let active_markets: Vec<_> = markets
            .into_iter()
            .filter(|m| m.active && m.status == "ACTIVE")
            .collect();

        info!("Fetching funding rates for {} active markets", active_markets.len());

        // Fetch funding rates concurrently
        let mut tasks = Vec::new();

        for market in active_markets {
            let market_name = market.name.clone();
            let client = self.clone_for_parallel();
            tasks.push(tokio::spawn(async move {
                (market_name.clone(), client.get_funding_rate(&market_name).await)
            }));
        }

        let mut funding_rates = Vec::new();
        for task in tasks {
            match task.await {
                Ok((market_name, result)) => match result {
                    Ok(Some(rate)) => funding_rates.push(rate),
                    Ok(None) => {
                        debug!("No funding rate data for {}", market_name);
                    }
                    Err(e) => {
                        warn!("Error fetching funding rate for {}: {}", market_name, e);
                    }
                },
                Err(e) => {
                    error!("Task join error: {}", e);
                }
            }
        }

        info!("Successfully fetched {} funding rates", funding_rates.len());
        Ok(funding_rates)
    }

    /// Get account information (requires API key)
    pub async fn get_account_info(&self) -> Result<AccountInfo> {
        let url = format!("{}/user/account/info", self.base_url);
        debug!("Fetching account info from {}", url);

        let api_key = self.api_key.as_ref().ok_or_else(|| {
            ConnectorError::ApiError("API key required for account info".to_string())
        })?;

        let response = self
            .client
            .get(&url)
            .header("X-Api-Key", api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            error!("API error: {} - {}", status, error_text);
            return Err(ConnectorError::ApiError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let api_response: ApiResponse<AccountInfo> = response.json().await?;

        match api_response.data {
            Some(account_info) => {
                info!(
                    "Fetched account info - ID: {}, Vault: {}, Status: {}",
                    account_info.account_id, account_info.l2_vault, account_info.status
                );
                Ok(account_info)
            }
            None => {
                let error_msg = api_response
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("API error response: {}", error_msg);
                Err(ConnectorError::ApiError(error_msg))
            }
        }
    }

    /// Get user positions, optionally filtered by market (requires API key)
    pub async fn get_positions(&self, market: Option<&str>) -> Result<Vec<Position>> {
        let url = if let Some(m) = market {
            format!("{}/user/positions?market={}", self.base_url, m)
        } else {
            format!("{}/user/positions", self.base_url)
        };
        debug!("Fetching positions from {}", url);

        let api_key = self.api_key.as_ref().ok_or_else(|| {
            ConnectorError::ApiError("API key required for positions".to_string())
        })?;

        let response = self
            .client
            .get(&url)
            .header("X-Api-Key", api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            error!("API error: {} - {}", status, error_text);
            return Err(ConnectorError::ApiError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let api_response: ApiResponse<Vec<Position>> = response.json().await?;

        match api_response.data {
            Some(positions) => {
                info!("Fetched {} positions", positions.len());
                Ok(positions)
            }
            None => {
                let error_msg = api_response
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("API error response: {}", error_msg);
                Err(ConnectorError::ApiError(error_msg))
            }
        }
    }

    /// Get account balance and margin information (requires API key)
    pub async fn get_balance(&self) -> Result<Balance> {
        let url = format!("{}/user/balance", self.base_url);
        debug!("Fetching balance from {}", url);

        let api_key = self.api_key.as_ref().ok_or_else(|| {
            ConnectorError::ApiError("API key required for balance".to_string())
        })?;

        let response = self
            .client
            .get(&url)
            .header("X-Api-Key", api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            error!("API error: {} - {}", status, error_text);
            return Err(ConnectorError::ApiError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let api_response: ApiResponse<Balance> = response.json().await?;

        match api_response.data {
            Some(balance) => {
                info!(
                    "Fetched balance - Equity: ${}, Available: ${}",
                    balance.equity, balance.available_for_trade
                );
                Ok(balance)
            }
            None => {
                let error_msg = api_response
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("API error response: {}", error_msg);
                Err(ConnectorError::ApiError(error_msg))
            }
        }
    }

    /// Update leverage for a specific market (requires API key)
    ///
    /// # Arguments
    /// * `market` - Market name (e.g., "BTC-USD", "ETH-USD")
    /// * `leverage` - New leverage value (e.g., "1", "5", "10")
    ///
    /// # Returns
    /// The updated leverage value for the market
    pub async fn update_leverage(&self, market: &str, leverage: &str) -> Result<String> {
        let url = format!("{}/user/leverage", self.base_url);
        debug!("Updating leverage for {} to {}x at {}", market, leverage, url);

        let api_key = self.api_key.as_ref().ok_or_else(|| {
            ConnectorError::ApiError("API key required for leverage update".to_string())
        })?;

        let request_body = serde_json::json!({
            "market": market,
            "leverage": leverage
        });

        debug!("Sending PATCH request: {}", request_body);

        let response = self
            .client
            .patch(&url)
            .header("X-Api-Key", api_key)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await?;

        // Get response text for debugging
        let response_text = response.text().await?;
        debug!("Leverage update response: {}", response_text);

        // Check if response is just a success status
        if response_text.contains("\"status\":\"OK\"") || response_text == "{\"status\":\"OK\"}" {
            info!("Successfully updated leverage for {} to {}x (OK response)", market, leverage);
            return Ok(leverage.to_string());
        }

        // Parse response
        #[derive(serde::Deserialize, Debug)]
        struct LeverageData {
            market: String,
            leverage: String,
        }

        let api_response: ApiResponse<LeverageData> = serde_json::from_str(&response_text)?;

        match api_response.data {
            Some(leverage_data) => {
                info!(
                    "Successfully updated leverage for {} to {}x",
                    leverage_data.market, leverage_data.leverage
                );
                Ok(leverage_data.leverage)
            }
            None => {
                let error_msg = api_response
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("API error response: {}", error_msg);
                Err(ConnectorError::ApiError(error_msg))
            }
        }
    }

    /// Get fee information for a market (requires API key)
    pub async fn get_fees(&self, market: &str) -> Result<FeeInfo> {
        let url = format!("{}/user/fees?market={}", self.base_url, market);
        debug!("Fetching fees for {} from {}", market, url);

        let api_key = self.api_key.as_ref().ok_or_else(|| {
            ConnectorError::ApiError("API key required for fee info".to_string())
        })?;

        let response = self
            .client
            .get(&url)
            .header("X-Api-Key", api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            error!("API error: {} - {}", status, error_text);
            return Err(ConnectorError::ApiError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        // Debug: print raw response
        let response_text = response.text().await?;
        debug!("Fee response: {}", response_text);

        let api_response: ApiResponse<FeeInfo> = serde_json::from_str(&response_text)?;

        match api_response.data {
            Some(fee_info) => {
                info!(
                    "Fetched fees for {} - Maker: {}, Taker: {}",
                    market, fee_info.maker_fee_str(), fee_info.taker_fee_str()
                );
                Ok(fee_info)
            }
            None => {
                let error_msg = api_response
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("API error response: {}", error_msg);
                Err(ConnectorError::ApiError(error_msg))
            }
        }
    }

    /// Get market configuration including L2 asset IDs and resolutions
    pub async fn get_market_config(&self, market: &str) -> Result<MarketConfig> {
        let url = format!("{}/info/markets", self.base_url);
        debug!("Fetching market config for {} from {}", market, url);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            error!("API error: {} - {}", status, error_text);
            return Err(ConnectorError::ApiError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let api_response: ApiResponse<Vec<MarketConfig>> = response.json().await?;

        match api_response.data {
            Some(markets) => {
                // Find the requested market
                let market_config = markets
                    .into_iter()
                    .find(|m| m.name == market)
                    .ok_or_else(|| {
                        ConnectorError::InvalidMarket(format!("Market {} not found", market))
                    })?;

                info!(
                    "Fetched config for {} - Synthetic: {}, Collateral: {}, SynRes: {}, ColRes: {}",
                    market,
                    market_config.l2_config.synthetic_id,
                    market_config.l2_config.collateral_id,
                    market_config.l2_config.synthetic_resolution,
                    market_config.l2_config.collateral_resolution
                );
                Ok(market_config)
            }
            None => {
                let error_msg = api_response
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("API error response: {}", error_msg);
                Err(ConnectorError::ApiError(error_msg))
            }
        }
    }

    /// Place a market order
    ///
    /// Parameters:
    /// - market: Market name (e.g., "SOL-USD")
    /// - side: Buy or Sell
    /// - notional_usd: Order size in USD (e.g., 15.0)
    /// - stark_private_key: Stark private key for signing
    /// - stark_public_key: Stark public key
    /// - vault_id: Collateral position ID
    /// - reduce_only: If true, order can only reduce existing position (for closing)
    pub async fn place_market_order(
        &self,
        market: &str,
        side: OrderSide,
        notional_usd: f64,
        stark_private_key: &str,
        stark_public_key: &str,
        vault_id: &str,
        reduce_only: bool,
        max_base_size: Option<f64>,
    ) -> Result<OrderResponse> {
        info!(
            "Placing market {} order on {} for ${:.2}",
            side, market, notional_usd
        );

        // 1. Get market configuration for asset IDs and resolutions
        let market_config = self.get_market_config(market).await?;
        let l2_config = &market_config.l2_config;

        // 2. Get current orderbook to calculate price and quantity
        let orderbook = self.get_orderbook(market).await?;

        if orderbook.bid.is_empty() || orderbook.ask.is_empty() {
            return Err(ConnectorError::ApiError(
                "Orderbook has no bids or asks".to_string(),
            ));
        }

        let best_bid: f64 = orderbook.bid[0].price.parse().map_err(|_| {
            ConnectorError::Other("Failed to parse best bid price".to_string())
        })?;
        let best_ask: f64 = orderbook.ask[0].price.parse().map_err(|_| {
            ConnectorError::Other("Failed to parse best ask price".to_string())
        })?;

        // 3. Calculate market order price
        // Buy: ask * 1.0075 (0.75% above best ask)
        // Sell: bid * 0.9925 (0.75% below best bid)
        let raw_price = match side {
            OrderSide::Buy => best_ask * 1.0075,
            OrderSide::Sell => best_bid * 0.9925,
        };

        // Get price precision from market config
        let price_precision = market_config.trading_config.get_price_precision();
        let price_multiplier = 10f64.powi(price_precision as i32);

        // Round price to correct precision BEFORE using it for calculations
        // This ensures our signed amounts match what the server recalculates
        let price = (raw_price * price_multiplier).round() / price_multiplier;

        // 4. Calculate quantity
        //    For reduce-only closes, derive quantity directly from the current
        //    base position size to avoid dust from price/notional rounding.
        //    For normal opens, if a desired base size is provided, use it directly
        //    so Extended matches the targeted base size precisely.
        let mut raw_quantity = if reduce_only {
            if let Some(max_base) = max_base_size { max_base } else { notional_usd / price }
        } else {
            if let Some(desired_base) = max_base_size { desired_base } else { notional_usd / price }
        };

        // Get trading config constraints
        let min_size: f64 = market_config.trading_config.min_order_size.parse()
            .map_err(|e| ConnectorError::Other(format!("Failed to parse minOrderSize: {}", e)))?;
        let size_increment: f64 = market_config.trading_config.min_order_size_change.parse()
            .map_err(|e| ConnectorError::Other(format!("Failed to parse minOrderSizeChange: {}", e)))?;

        // Round quantity according to context:
        // - Normal orders: nearest increment
        // - Reduce-only: round DOWN to avoid exceeding current position size
        let mut quantity = if reduce_only {
            (raw_quantity / size_increment).floor() * size_increment
        } else {
            (raw_quantity / size_increment).round() * size_increment
        };

        // For normal orders, enforce exchange minimum. For reduce-only, avoid bumping up which can exceed size.
        if !reduce_only {
            // NOTE: this bump is applied to the EXTENDED leg only. If it raises the
            // size above what the Pacifica leg will carry, the pair is no longer
            // delta-neutral -- the caller must compare the two fills rather than
            // assume the requested size was used.
            quantity = quantity.max(min_size);
        }

        // Reject a zero/negative quantity HERE rather than letting it reach the
        // signer.
        //
        // calculate_signed_amounts() does `assert!(quantity > 0.0)`, and an assert
        // is a PANIC, not an error: it kills the process. Reduce-only floors without
        // a minimum bump (see above), so any dust position smaller than one size
        // increment rounds to exactly 0.0 -- and the panic then happens midway
        // through a close, taking the process down with the OTHER leg still open,
        // straight into `restart: unless-stopped`, which re-panics on the same dust.
        //
        // A dust residual is a normal, recoverable condition. It must surface as an
        // error the caller can handle, never as process death.
        if !(quantity > 0.0) {
            return Err(ConnectorError::Other(format!(
                "Refusing to submit a zero-size order for {} (raw={}, increment={}, \
                 reduce_only={}). The remaining position is smaller than one size \
                 increment; it must be closed manually or left as dust.",
                market, raw_quantity, size_increment, reduce_only
            )));
        }

        // Calculate formatting precisions
        let qty_precision = if size_increment >= 1.0 {
            0
        } else {
            (-size_increment.log10()).ceil() as usize
        };
        let price_precision = market_config.trading_config.get_price_precision();

        // Format and parse back to get EXACT values that will be sent to the server
        // This ensures our signature calculation matches the server's
        let quantity_formatted = format!("{:.prec$}", quantity, prec = qty_precision);
        let price_formatted = format!("{:.prec$}", price, prec = price_precision);
        let quantity_exact: f64 = quantity_formatted.parse().unwrap();
        let price_exact: f64 = price_formatted.parse().unwrap();

        info!(
            "Order details - Price: ${:.2}, Raw Qty: {:.6}, Rounded Qty: {}, Min: {}, Best Bid: ${:.2}, Best Ask: ${:.2}",
            price, raw_quantity, quantity, min_size, best_bid, best_ask
        );

        // 5. Get taker fee
        let fee_info = self.get_fees(market).await?;
        let taker_fee_rate: f64 = fee_info.taker_fee_str().parse().unwrap_or(0.0006);

        // 6. Calculate signed amounts using EXACT formatted values
        let (base_amount, quote_amount, fee_amount) = crate::signature::calculate_signed_amounts(
            &side,
            quantity_exact,
            price_exact,
            taker_fee_rate,
            l2_config.synthetic_resolution,
            l2_config.collateral_resolution,
        );

        info!(
            "Stark amounts - Base: {}, Quote: {}, Fee: {}",
            base_amount, quote_amount, fee_amount
        );

        // 7. Generate order ID and nonce
        // Use a simpler timestamp-based ID
        let now = chrono::Utc::now();
        let order_id = format!("rust-{}", now.timestamp_millis());
        // Nonce must be between 1 and 2^31 per Extended API requirements
        // Use timestamp in seconds (current ~1.7B, fits well under 2^31 = 2.1B)
        let nonce = now.timestamp() as u64;

        // Do not log nonce values to avoid leaking signing metadata

        // 8. Set expiry (1 hour from now for market order)
        let expiry_epoch_millis = (chrono::Utc::now().timestamp_millis() + (3600 * 1000)) as u64;

        // 9. Determine environment (mainnet vs testnet)
        let domain_chain_id = if self.base_url.contains("sepolia") {
            "SN_SEPOLIA"
        } else {
            "SN_MAIN"
        };

        // 10. Sign the order using Python SDK
        let signature = crate::signature::sign_order(
            &l2_config.synthetic_id,
            &l2_config.collateral_id,
            base_amount,
            quote_amount,
            fee_amount,
            vault_id.parse().unwrap_or(0),
            nonce,
            expiry_epoch_millis,
            stark_public_key,
            stark_private_key,
            domain_chain_id,
        )?;

        // 11. Create order request
        // Note: Market orders are implemented as limit IOC orders with aggressive pricing

        info!("Formatting - Qty precision: {}, Price precision: {}", qty_precision, price_precision);

        let order_request = OrderRequest {
            id: order_id.clone(),
            market: market.to_string(),
            order_type: OrderType::Limit,  // Market orders use Limit type with IOC
            side: side.clone(),
            qty: quantity_formatted,
            price: price_formatted,
            time_in_force: TimeInForce::IOC,
            expiry_epoch_millis,
            fee: format!("{:.6}", taker_fee_rate),  // Fee RATE, not calculated amount
            nonce: nonce.to_string(),
            settlement: Settlement {
                signature,
                stark_key: stark_public_key.to_string(),
                collateral_position: vault_id.to_string(),
            },
            self_trade_protection_level: "ACCOUNT".to_string(),
            reduce_only,
            post_only: false,
        };

        // 9. Submit order
        let url = format!("{}/user/order", self.base_url);
        debug!("Submitting order to {} (id: {}, market: {}, side: {})",
            url, order_id, market, match side { OrderSide::Buy => "BUY", OrderSide::Sell => "SELL" });

        let api_key = self.api_key.as_ref().ok_or_else(|| {
            ConnectorError::ApiError("API key required for order placement".to_string())
        })?;

        let response = self
            .client
            .post(&url)
            .header("X-Api-Key", api_key)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .json(&order_request)
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;

        debug!("Order response status: {}", status);
        // Do not print full response body at info level; debug-only is acceptable and typically non-sensitive
        debug!("Order response body: {}", response_text);

        if !status.is_success() {
            error!("Order placement failed: {} - {}", status, response_text);
            return Err(ConnectorError::ApiError(format!(
                "HTTP {}: {}",
                status, response_text
            )));
        }

        let api_response: ApiResponse<OrderResponse> = serde_json::from_str(&response_text)
            .map_err(|e| {
                ConnectorError::Other(format!(
                    "Failed to parse order response: {}. Response: {}",
                    e, response_text
                ))
            })?;

        match api_response.data {
            Some(order_response) => {
                info!(
                    "Order placed successfully - Order ID: {}, External ID: {}",
                    order_response.id, order_response.external_id
                );
                Ok(order_response)
            }
            None => {
                let error_msg = api_response
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("API error response: {}", error_msg);
                Err(ConnectorError::ApiError(error_msg))
            }
        }
    }

    /// Close an existing position by placing a reduce-only market order
    ///
    /// # Arguments
    /// - position: The position to close
    /// - stark_private_key: Stark private key for signing
    /// - stark_public_key: Stark public key
    /// - vault_id: Collateral position ID
    ///
    /// # Returns
    /// OrderResponse if successful
    pub async fn close_position(
        &self,
        position: &Position,
        stark_private_key: &str,
        stark_public_key: &str,
        vault_id: &str,
    ) -> Result<OrderResponse> {
        // Determine opposite side to close the position
        let close_side = match position.side {
            crate::types::PositionSide::Long => OrderSide::Sell,
            crate::types::PositionSide::Short => OrderSide::Buy,
        };

        // Get position value in USD
        let position_value_usd = position.value_f64();

        info!(
            "Closing {:?} position on {} (value: ${:.2})",
            position.side, position.market, position_value_usd
        );

        // Place reduce-only market order to close
        self.place_market_order(
            &position.market,
            close_side,
            position_value_usd,
            stark_private_key,
            stark_public_key,
            vault_id,
            true, // reduce_only = true
            Some(position.size_f64()),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_orderbook() {
        let client = RestClient::new_mainnet(None).unwrap();
        let result = client.get_orderbook("BTC-USD").await;

        match result {
            Ok(orderbook) => {
                assert_eq!(orderbook.market, "BTC-USD");
                assert!(!orderbook.bid.is_empty(), "Bid should not be empty");
                assert!(!orderbook.ask.is_empty(), "Ask should not be empty");
                println!("Orderbook: {:?}", orderbook);
            }
            Err(e) => {
                println!("Error fetching orderbook (might be expected in test environment): {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_get_bid_ask() {
        let client = RestClient::new_mainnet(None).unwrap();
        let result = client.get_bid_ask("BTC-USD").await;

        match result {
            Ok(bid_ask) => {
                println!("{}", bid_ask);
                assert_eq!(bid_ask.market, "BTC-USD");
            }
            Err(e) => {
                println!("Error fetching bid/ask: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_multiple_markets() {
        let client = RestClient::new_mainnet(None).unwrap();
        let markets = vec!["BTC-USD".to_string(), "ETH-USD".to_string()];
        let results = client.get_multiple_bid_asks(&markets).await;

        assert_eq!(results.len(), 2);

        for result in results {
            match result {
                Ok(bid_ask) => println!("{}", bid_ask),
                Err(e) => println!("Error: {}", e),
            }
        }
    }
}
