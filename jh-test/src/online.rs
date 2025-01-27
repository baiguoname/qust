use super::di::*;



pub trait UpdateOnline<T> {
    fn update_online(&mut self, input: T);
}

impl UpdateOnline<KlineDataTd> for DiTd {
    fn update_online(&mut self, input: KlineDataTd) {
        self.di.pcon.price.update(&input.kline_data);
        self.td.push(input.td);
    }
}

impl UpdateOnline<(&str, KlineDataTd)> for DiContracts {
    fn update_online(&mut self, input: (&str, KlineDataTd)) {
        self.pool
            .get_mut(input.0)
            .unwrap()
            .update_online(input.1);
    }
}