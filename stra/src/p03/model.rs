use burn::{
    nn::{
        loss::{MseLoss, Reduction::Mean},
        Linear, LinearConfig, Relu,
    },
    prelude::*,
    tensor::backend::AutodiffBackend,
    train::{RegressionOutput, TrainOutput, TrainStep, ValidStep},
};
use self::nn::{LeakyRelu, Lstm, Sigmoid};

use super::dataset::*;

#[derive(Module, Debug)]
pub struct RegressionModel<B: Backend> {
    input_layer: Linear<B>,
    mid_layer1: Linear<B>,
    mid_layer2: Linear<B>,
    output_layer: Linear<B>,
    activation: Relu,
}

#[derive(Config)]
pub struct RegressionModelConfig {
    pub num_features: usize,

    #[config(default = 1000)]
    pub mid_size1: usize,

    #[config(default = 500)]
    pub mid_size2: usize,

    #[config(default = 100)]
    pub hidden_size: usize,
}

impl RegressionModelConfig {
    pub fn init<B: Backend>(&self, device: &B::Device) -> RegressionModel<B> {
        let input_layer = LinearConfig::new(self.num_features, self.mid_size1)
            .with_bias(false)
            .init(device);
        let mid_layer1 = LinearConfig::new(self.mid_size1, self.mid_size2)
            .with_bias(false)
            .init(device);
        let mid_layer2 = LinearConfig::new(self.mid_size2, self.hidden_size)
            .with_bias(false)
            .init(device);
        let output_layer = LinearConfig::new(self.hidden_size, 1)
            .with_bias(false)
            .init(device);

        RegressionModel {
            input_layer,
            mid_layer1,
            mid_layer2,
            output_layer,
            activation: Relu::new(),
        }
    }
}

impl<B: Backend> RegressionModel<B> {
    pub fn forward(&self, input: Tensor<B, 2>) -> Tensor<B, 2> {
        let mut x = input.detach();
        x = self.input_layer.forward(x);
        x = self.mid_layer1.forward(x);
        x = self.mid_layer2.forward(x);
        x = self.activation.forward(x);
        self.output_layer.forward(x)
    }

    pub fn forward_step(&self, item: DiabetesBatch<B>) -> RegressionOutput<B> {
        let targets: Tensor<B, 2> = item.targets.unsqueeze_dim(1);
        let output: Tensor<B, 2> = self.forward(item.inputs);

        let loss = MseLoss::new().forward(output.clone(), targets.clone(), Mean);

        RegressionOutput {
            loss,
            output,
            targets,
        }
    }
}

impl<B: AutodiffBackend> TrainStep<DiabetesBatch<B>, RegressionOutput<B>> for RegressionModel<B> {
    fn step(&self, item: DiabetesBatch<B>) -> TrainOutput<RegressionOutput<B>> {
        let item = self.forward_step(item);

        TrainOutput::new(self, item.loss.backward(), item)
    }
}

impl<B: Backend> ValidStep<DiabetesBatch<B>, RegressionOutput<B>> for RegressionModel<B> {
    fn step(&self, item: DiabetesBatch<B>) -> RegressionOutput<B> {
        self.forward_step(item)
    }
}
