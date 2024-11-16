use self::nn::transformer::TransformerEncoderConfig;

use super::dataset::{DiabetesBatcher, DiabetesDataset};
use super::model::{RegressionModel, RegressionModelConfig};
use burn::backend::ndarray::{NdArrayDevice, NdArrayTensor};
use burn::backend::{Autodiff, NdArray};
use burn::data::dataloader::batcher::Batcher;
use burn::lr_scheduler::exponential::ExponentialLrSchedulerConfig;
use burn::optim::AdamConfig;
use burn::tensor::{BasicAutodiffOps, DType, Data, Element};
use burn::{
    data::{dataloader::DataLoaderBuilder, dataset::Dataset},
    optim::SgdConfig,
    prelude::*,
    record::{CompactRecorder, NoStdTrainingRecorder},
    tensor::backend::AutodiffBackend,
    train::{
        metric::store::{Aggregate, Direction, Split},
        metric::LossMetric,
        LearnerBuilder, MetricEarlyStoppingStrategy, StoppingCondition,
    },
};
use qust::prelude::*;
use super::datasetc::*;
use super::modelc::*;

static ARTIFACT_DIR: &str = "/root/qust/notebook/git_test/model2";
const features_len: usize = 150;

#[derive(Config)]
pub struct ClassConfig {
    pub optimizer: AdamConfig,
    #[config(default = 3)]
    pub n_classes: usize,
    #[config(default = 1000)]
    pub batch_size: usize,
    #[config(default = 10)]
    pub num_epochs: usize,
    #[config(default = 10)]
    pub num_workers: usize
}


pub fn run<B: AutodiffBackend>(device: B::Device) {
    ARTIFACT_DIR.build_an_empty_dir();
    // let trans_config = TransformerEncoderConfig::new(1, 1, 1, 1);
    let config = AdamConfig::new();
    let model_config = ClassConfig::new(config);
    let model = ClassModelConfig::new(features_len).init_linear(&device);

    let batcher_train = ClassDataBatcher::<B>::new(device.clone());
    let batcher_test = ClassDataBatcher::<B::InnerBackend>::new(device.clone());

    let dataset_train = ClassDataSet::new_trunc(20240101.to_da().to(20240201.to_da()));
    println!("ctp {:?}", dataset_train.dataset.iter().map(|x| x.label).counts());
    let dataset_test = ClassDataSet::default();

    let dataloader_train = DataLoaderBuilder::new(batcher_train)
        // .batch_size(model_config.batch_size)
        .batch_size(20_000)
        .shuffle(10)
        .num_workers(model_config.num_workers)
        .build(dataset_train);
    let dataloader_test = DataLoaderBuilder::new(batcher_test)
        // .batch_size(model_config.batch_size)
        .batch_size(20_000)
        .shuffle(10)
        .num_workers(model_config.num_workers)
        .build(dataset_test);

    let learner = LearnerBuilder::new(ARTIFACT_DIR)
        .metric_train_numeric(LossMetric::new())
        // .metric_valid_numeric(LossMetric::new())
        // .with_file_checkpointer(CompactRecorder::new())
        // .early_stopping(MetricEarlyStoppingStrategy::new::<LossMetric<B>>(
        //     Aggregate::Mean,
        //     Direction::Lowest,
        //     Split::Valid,
        //     StoppingCondition::NoImprovementSince { n_epochs: 1 },
        // ))
        .devices(vec![device.clone()])
        // .num_epochs(model_config.num_epochs)
        .num_epochs(15)
        .summary()
        // .build(model, config.optimizer.init(), ExponentialLrSchedulerConfig::new(0.05, 0.1).init());
        .build(model, model_config.optimizer.init(), 5e-4);

    let model_trained = learner.fit(dataloader_train, dataloader_test);

    model_trained
        .save_file(
            format!("{ARTIFACT_DIR}/model"),
            &NoStdTrainingRecorder::new(),
        )
        .expect("Failed to save trained model");
}

pub fn predict() {
    let device = NdArrayDevice::Cpu;
    let model_trained = get_model(&device);
    // let predict_data = DiabetesBatcher::new(device.clone());
    // let predict_data = ClassDataSet::new(20240101.to_da().to(20240201.to_da()));
    let predict_data = ClassDataSet::new(20240201.to_da().to(20240301.to_da()));
    let batcher_train = ClassDataBatcher::new(device);
    let bb = batcher_train.batch(predict_data.dataset);
    let predicted_res = model_trained.forward(bb);
    let k = predicted_res.output.inner().into_primitive();
    let g1 = k.tensor().array.into_iter().collect_vec().chunks(3).map(|x| x.to_vec()).collect_vec();
    let g2 = predicted_res.targets.inner().into_primitive().array.into_iter().collect_vec();
    let g3 = predict_data.time_vec;
    g1.sof("g1", "/root/qust/notebook/git_test");
    g2.sof("g2", "/root/qust/notebook/git_test");
    g3.sof("g3", "/root/qust/notebook/git_test");
}


pub fn get_model(device: &NdArrayDevice) -> ClassModel2<Autodiff<NdArray>> {
    // let class_config = ClassConfig::new(config);
    let model: ClassModel2<Autodiff<NdArray>> = ClassModelConfig::new(features_len).init_linear(device);
    model.load_file(
        format!("{ARTIFACT_DIR}/model"),
        &NoStdTrainingRecorder::new(),
        device,
    ).unwrap()
}