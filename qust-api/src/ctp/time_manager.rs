use chrono::Local;
use qust::prelude::*;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, Default)]
pub enum RunningState {
    InRunningInTradingTime,
    InRunningNotTradingTime,
    NotRunningInTradingTime,
    #[default]
    NotRunningNotTradingTime,
}

#[derive(Debug, Clone)]
pub enum RunningAction {
    StartToRun(RunningState),
    StopToRun(RunningState),
    Sleep(u64, String),
    Impossible,
}

use RunningState::*;
use RunningAction::*;

pub struct TimeManager {
    pub time_intervals: Vec<ForCompare<tt>>, 
    pub last_running_state: RunningState,
}

impl TimeManager {
    pub fn get_state(&self) -> RunningAction {
        let time_now = Local::now().time();
        let is_in_trading_time = self.time_intervals.iter().any(|x| x.compare_same(&time_now));
        match (self.last_running_state, is_in_trading_time) {
            (InRunningInTradingTime, true) => Sleep(300, "in running and in trading, get in trading".into()),
            (InRunningInTradingTime, false) => StopToRun(NotRunningNotTradingTime),
            (InRunningNotTradingTime, true) => Impossible,
            (InRunningNotTradingTime, false) => StopToRun(NotRunningNotTradingTime),
            (NotRunningInTradingTime, true) => StartToRun(InRunningInTradingTime),
            (NotRunningInTradingTime, false) => Sleep(300,  "not running and in trading, get no trading".into()),
            (NotRunningNotTradingTime, true) => StartToRun(InRunningInTradingTime),
            (NotRunningNotTradingTime, false) => Sleep(300, "not running not in trading, get no trading".into()),
        }
    }
}


impl Default for TimeManager {
    fn default() -> Self {
        let trade_time_interval = vec![
            Between(84000.to_tt()..145900.to_tt()),
            Between(204000.to_tt()..235959.to_tt()),
            // Between(112030.to_tt()..112159.to_tt()),
            // Between(112230.to_tt()..112359.to_tt()),
            // Between(112430.to_tt()..112559.to_tt()),
            // Between(112630.to_tt()..112759.to_tt()),
            // Between(115830.to_tt()..115959.to_tt()),
            // Between(115030.to_tt()..115159.to_tt()),
            // Between(115230.to_tt()..115359.to_tt()),
            // Between(115430.to_tt()..115559.to_tt()),
            // Between(115630.to_tt()..115759.to_tt()),
            // Between(115830.to_tt()..115959.to_tt()),
        ];
        Self {
            time_intervals: trade_time_interval,
            last_running_state: Default::default(),
        }
    }
}