use burn::data::dataset::Dataset;
use burn::{
    data::dataloader::batcher::Batcher,
    prelude::*,
};
use qust::prelude::*;

use super::p03::NUM_FEATURES;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DiabetesItem(pub v32, pub f32);


#[derive(Default)]
pub struct DiabetesDataset {
    pub dataset: Vec<DiabetesItem>,
    pub time_vec: vdt,
}

impl Dataset<DiabetesItem> for DiabetesDataset {
    fn get(&self, index: usize) -> Option<DiabetesItem> {
        Some(self.dataset[index].clone())
    }

    fn len(&self) -> usize {
        self.dataset.len()
    }
}

impl DiabetesDataset {
    pub fn new(range: ForCompare<dt>) -> Self {
        use super::p03::GetTrainData;

        // let data = GetTrainData::Binary.get_xy_data([ler, SAer, eger], range);
        let data = GetTrainData::Contin.get_xy_data(vec![ver, RMer, eger, ler, SAer, eber], range);
        // let data = GetTrainData::Contin.get_xy_data(vec![ver, SAer], range);
        let mut res = Vec::with_capacity(data.0.len());
        data.0.into_iter().zip(data.1)
            .for_each(|(x, y)| {
                let res_part = DiabetesItem(x, y);
                res.push(res_part);
            });
        DiabetesDataset { dataset: res, time_vec: data.2 }
    }
}

#[derive(Clone, Debug)]
pub struct DiabetesBatcher<B: Backend> {
    device: B::Device,
}

#[derive(Clone, Debug)]
pub struct DiabetesBatch<B: Backend> {
    pub inputs: Tensor<B, 2>,
    pub targets: Tensor<B, 1>,
}

impl<B: Backend> DiabetesBatcher<B> {
    pub fn new(device: B::Device) -> Self {
        Self { device }
    }
}

impl<B: Backend> Batcher<DiabetesItem, DiabetesBatch<B>> for DiabetesBatcher<B> {
    fn batch(&self, items: Vec<DiabetesItem>) -> DiabetesBatch<B> {
        let rows = items.len();
        let mut inputs = Vec::with_capacity(rows);
        let mut targets = Vec::with_capacity(rows);

        for x_y in items.into_iter() {
            let b: [f32; NUM_FEATURES] = x_y.0.try_into().unwrap();
            let input_tensor = Tensor::<B, 1>::from_floats(b, &self.device);
            let target_tensor = Tensor::<B, 1>::from_floats([x_y.1], &self.device);
            inputs.push(input_tensor.unsqueeze());
            targets.push(target_tensor);
        }
        let inputs = Tensor::cat(inputs, 0);
        let targets = Tensor::cat(targets, 0);
        DiabetesBatch {
            inputs,
            targets,
        }
    }
}