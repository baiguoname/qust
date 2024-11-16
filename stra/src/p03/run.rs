use crate::p03::p03::NUM_FEATURES;

use super::dataset::{DiabetesBatcher, DiabetesDataset};
use super::model::{RegressionModel, RegressionModelConfig};
use burn::backend::ndarray::NdArrayDevice;
use burn::backend::{Autodiff, NdArray};
use burn::data::dataloader::batcher::Batcher;
use burn::grad_clipping::{GradientClipping, GradientClippingConfig};
use burn::lr_scheduler::exponential::ExponentialLrSchedulerConfig;
use burn::optim::decay::WeightDecayConfig;
use burn::optim::momentum::MomentumConfig;
use burn::optim::{Optimizer, SimpleOptimizer};
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

static ARTIFACT_DIR: &str = "/root/qust/notebook/git_test/model2";

#[derive(Config)]
pub struct ExpConfig {
    #[config(default = 15)]
    pub num_epochs: usize,

    #[config(default = 10)]
    pub num_workers: usize,

    #[config(default = 42)]
    pub seed: u64,

    pub optimizer: SgdConfig,
}

pub fn run<B: AutodiffBackend>(device: B::Device) {
    // Config
    ARTIFACT_DIR.build_an_empty_dir();
    let optimizer = SgdConfig::new()
        .with_weight_decay(Some(WeightDecayConfig { penalty: 0.} ))
        .with_momentum(Some(MomentumConfig {
            momentum: 0.1,
            dampening: 0.01,
            nesterov: true,
        }))
        .with_gradient_clipping(Some(GradientClippingConfig::Norm(10.00)));
    // let optimizer = SgdConfig::new()
    //     .with_gradient_clipping(None)
    //     .with_momentum(None)
    //     .with_weight_decay(None);
    let config = ExpConfig::new(optimizer);
    let model = RegressionModelConfig::new(NUM_FEATURES).init(&device);
    B::seed(config.seed);

    // Define train/test datasets and dataloaders

    let train_dataset = DiabetesDataset::new(20240101.to_da().to(20240201.to_da()));
    // let test_dataset = DiabetesDataset::new(20240301.to_da().to(20240401.to_da()));
    let test_dataset = DiabetesDataset::default();

    println!("Train Dataset Size: {}", train_dataset.len());
    println!("Test Dataset Size: {}", test_dataset.len());

    let batcher_train = DiabetesBatcher::<B>::new(device.clone());

    let batcher_test = DiabetesBatcher::<B::InnerBackend>::new(device.clone());

    // Since dataset size is small, we do full batch gradient descent and set batch size equivalent to size of dataset

    let dataloader_train = DataLoaderBuilder::new(batcher_train)
        .batch_size(10000)
        .shuffle(config.seed)
        .num_workers(config.num_workers)
        .build(train_dataset);

    let dataloader_test = DataLoaderBuilder::new(batcher_test)
        .batch_size(10000)
        .shuffle(config.seed)
        .num_workers(config.num_workers)
        .build(test_dataset);

    // Model
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
        .num_epochs(config.num_epochs)
        .summary()
        // .build(model, config.optimizer.init(), ExponentialLrSchedulerConfig::new(0.05, 0.1).init());
        .build(model, config.optimizer.init(), 5e-2);


    let model_trained = learner.fit(dataloader_train, dataloader_test);

    config
        .save(format!("{ARTIFACT_DIR}/config.json").as_str())
        .unwrap();

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
    let predict_data = DiabetesDataset::new(20240101.to_da().to(20240201.to_da()));
    let batcher_train = DiabetesBatcher::new(device);
    let bb = batcher_train.batch(predict_data.dataset);
    let predicted_res = model_trained.forward(bb.inputs);
    let k = predicted_res.inner().into_primitive();
    let g1 = k.tensor().array.into_iter().collect_vec();
    let g2 = bb.targets.inner().into_primitive().tensor().array.into_iter().collect_vec();
    let g3 = predict_data.time_vec;
    g1.sof("g1", "/root/qust/notebook/git_test");
    g2.sof("g2", "/root/qust/notebook/git_test");
    g3.sof("g3", "/root/qust/notebook/git_test");
}


pub fn get_model(device: &NdArrayDevice) -> RegressionModel<Autodiff<NdArray>> {
    let model: RegressionModel<Autodiff<NdArray>> = RegressionModelConfig::new(NUM_FEATURES).init(device);
    model.load_file(
        format!("{ARTIFACT_DIR}/model"),
        &NoStdTrainingRecorder::new(),
        device,
    ).unwrap()
}