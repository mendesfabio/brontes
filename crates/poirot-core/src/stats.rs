use crate::errors::TraceParseError;
use tracing::{
    field::{Field, Visit},
    span::Attributes,
    Id, Subscriber,
};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

pub struct ParserStatsLayer;

#[derive(Default, Debug)]
pub struct ParserStats {
    pub total_tx: usize,
    pub total_traces: usize,
    pub successful_parses: usize,
    pub not_recognized_action_errors: usize,
    pub empty_input_errors: usize,
    pub etherscan_errors: usize,
    pub abi_parse_errors: usize,
    pub invalid_function_selector_errors: usize,
    pub abi_decoding_failed_errors: usize,
    pub trace_missing_errors: usize,
}

impl<S> Layer<S> for ParserStatsLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, _attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).unwrap();

        span.extensions_mut().insert(ParserStats::default);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        if let Some(id) = ctx.current_span().id() {
            let span = ctx.span(id).unwrap();
            if let Some(ext) = span.extensions_mut().get_mut::<ParserStats>() {
                event.record(&mut *ext);
            };
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).unwrap();
        let binding = span.extensions();
        let stats = binding.get::<ParserStats>().unwrap();

        println!(
            "Total Transactions: {}\n 
            Total Traces: {}\n
            Successful Parses: {}\n
            Not Recognized Action Errors: {}\n
            Empty Input Errors: {}\n
            Etherscan Errors: {}\n
            ABI Parse Errors: {}\n
            Invalid Function Selector Errors: {}\n
            ABI Decoding Failed Errors: {}\n
            Trace Missing Errors: {}\n",
            stats.total_tx,
            stats.total_traces,
            stats.successful_parses,
            stats.not_recognized_action_errors,
            stats.empty_input_errors,
            stats.etherscan_errors,
            stats.abi_parse_errors,
            stats.invalid_function_selector_errors,
            stats.abi_decoding_failed_errors,
            stats.trace_missing_errors
        );
    }
}

impl Visit for ParserStats {
    /// will implement incrementing counters for tx/block traces
    /// tbd
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        todo!()
    }

    fn record_error(&mut self, _field: &Field, value: &(dyn std::error::Error + 'static)) {
        if let Some(error) = value.downcast_ref::<TraceParseError>() {
            match error {
                TraceParseError::TraceMissing => self.trace_missing_errors += 1,
                TraceParseError::NotRecognizedAction(_) => self.not_recognized_action_errors += 1,
                TraceParseError::EmptyInput(_) => self.empty_input_errors += 1,
                TraceParseError::EtherscanError(_) => self.etherscan_errors += 1,
                TraceParseError::AbiParseError(_) => self.abi_parse_errors += 1,
                TraceParseError::InvalidFunctionSelector(_) => self.abi_parse_errors += 1,
                TraceParseError::AbiDecodingFailed(_) => self.abi_decoding_failed_errors += 1,
            }
        }
    }
}