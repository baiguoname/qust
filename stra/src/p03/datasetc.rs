use burn::data::dataset::Dataset;
use burn::{
    data::dataloader::batcher::Batcher,
    prelude::*,
};
use qust::prelude::*;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClassDataItem {
    pub inputs: v32,
    pub label: i64,
}


#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct ClassDataSet {
    pub dataset: Vec<ClassDataItem>,
    pub time_vec: vdt,
}

impl Dataset<ClassDataItem> for ClassDataSet {
    fn get(&self, index: usize) -> Option<ClassDataItem> {
        self.dataset.get(index).cloned()
    }

    fn len(&self) -> usize {
        self.dataset.len()
    }
}

impl ClassDataSet {
    pub fn new(range: ForCompare<dt>) -> Self {
        use super::p03::GetTrainData;

        let data = GetTrainData::Binary.get_xy_data([ver, RMer, eger], range);
        let mut res = Vec::with_capacity(data.0.len());
        data.0.into_iter().zip(data.1)
            .for_each(|(x, y)| {
                let res_part = ClassDataItem { inputs: x, label: y as i64 };
                res.push(res_part);
            });
        Self { dataset: res, time_vec: data.2 }
    }

    pub fn new_trunc(range: ForCompare<dt>) -> Self {
        let res = Self::new(range);
        let mut kv = res.dataset.iter().map(|x| x.label).counts();
        let (_, v) = kv
            .clone()
            .into_iter()
            .min_by_key(|x| x.1)
            .unwrap();
        kv.iter_mut().for_each(|(_, y)| {
            *y = 0;
        });
        let mut dataset = vec![];
        let mut time_vec = vec![];
        for (data, t) in res.dataset.into_iter().zip(res.time_vec.into_iter()) {
            let label = data.label;
            if kv[&data.label] < v {
                dataset.push(data);
                time_vec.push(t);
                let g = kv.get_mut(&label).unwrap();
                *g += 1;
            }
        }
        Self {
            dataset,
            time_vec
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClassDataBatcher<B: Backend> {
    device: B::Device,
}

impl<B: Backend> ClassDataBatcher<B> {
    pub fn new(device: B::Device) -> Self {
        Self { device }
    }

}

#[derive(Debug, Clone)]
pub struct ClassDataBatch<B: Backend> {
    pub inputs: Tensor<B, 2>,
    pub labels: Tensor<B, 1, Int>,
}

impl<B: Backend> Batcher<ClassDataItem, ClassDataBatch<B>> for ClassDataBatcher<B> {
    fn batch(&self, items: Vec<ClassDataItem>) -> ClassDataBatch<B> {
        let rows = items.len();
        let mut inputs = Vec::with_capacity(rows);
        let mut labels = Vec::with_capacity(rows);

        for x_y in items.into_iter() {
            let b: [f32; 150] = x_y.inputs.try_into().expect("zzzz");
            let input_tensor = Tensor::<B, 1>::from_floats(b, &self.device);
            inputs.push(input_tensor.unsqueeze());
            labels.push(Tensor::from_data(
                TensorData::from([(x_y.label).elem::<B::IntElem>()]),
                &self.device,
            ));
        }
        let inputs = Tensor::cat(inputs, 0);
        let labels = Tensor::cat(labels, 0);
        ClassDataBatch {
            inputs,
            labels,
        }
    }
}

