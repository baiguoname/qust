use burn::{
    nn::{
        loss::{MseLoss, Reduction::Mean},
        Linear, LinearConfig, Relu, InstanceNorm,
    },
    prelude::*,
    tensor::backend::AutodiffBackend,
    train::{ClassificationOutput, RegressionOutput, TrainOutput, TrainStep, ValidStep},
};
use qust::loge;
use self::nn::{loss::CrossEntropyLossConfig, transformer::TransformerEncoder, Embedding, InstanceNormConfig, LeakyRelu, Lstm, LstmConfig, Sigmoid};
use super::datasetc::*;
use burn::tensor::activation::softmax;

#[derive(Module, Debug)]
pub struct ClassModel<B: Backend> {
    transformer: Lstm<B>,
    input_layer1: Lstm<B>,
    output_layer: Linear<B>,
    n_classes: usize,
    num_features: usize,
}

#[derive(Module, Debug)]
pub struct ClassModel2<B: Backend> {
    layer1: Linear<B>,
    layer2: Linear<B>,
    layer3: Linear<B>,
    layer4: Linear<B>,
    output_layer: Linear<B>,
    n_classes: usize,
    num_features: usize,
}

#[derive(Module, Debug)]
pub struct ClassModel3<B: Backend> {
    layer1: Embedding<B>,
    layer2: InstanceNorm<B>,
    layer3: InstanceNorm<B>,
    layer4: InstanceNorm<B>,
}

// #[derive(Config)]
// pub struct ClassModelConfig  {
//     pub num_features: usize,
//     #[config(default = 50)]
//     pub hidden_size: usize,
//     #[config(default = 3)]
//     pub n_classes: usize,
// }

#[derive(Config)]
pub struct ClassModelConfig  {
    pub num_features: usize,
    #[config(default = 1000)]
    pub layer1: usize,
    #[config(default = 500)]
    pub layer2: usize,
    #[config(default = 200)]
    pub layer3: usize,
    #[config(default = 100)]
    pub layer4: usize,
    #[config(default = 3)]
    pub n_classes: usize,
}

impl ClassModelConfig {
    pub fn init<B: Backend>(&self, device: &B::Device) -> ClassModel<B> {
        let transformer = LstmConfig::new(self.num_features, self.layer4, false).init(device);
        let input_layer1 = LstmConfig::new(100, self.layer4, false).init(device);
        let output_layer = LinearConfig::new(self.layer1, self.n_classes)
            .with_bias(true)
            .init(device);
        ClassModel {
            transformer,
            input_layer1,
            output_layer,
            n_classes: self.n_classes,
            num_features: self.num_features,
        }
    }

    pub fn init_linear<B: Backend>(&self, device: &B::Device) -> ClassModel2<B> {
        let layer1 = LinearConfig::new(self.num_features, self.layer1)
            .with_bias(false)
            .init(device);
        let layer2 = LinearConfig::new(self.layer1, self.layer2)
            .with_bias(false)
            .init(device);
        let layer3 = LinearConfig::new(self.layer2, self.layer3)
            .with_bias(false)
            .init(device);
        let layer4 = LinearConfig::new(self.layer3, self.layer4)
            .with_bias(false)
            .init(device);
        let output_layer = LinearConfig::new(self.layer4, self.n_classes)
            .with_bias(false)
            .init(device);
        ClassModel2 {
            layer1,
            layer2,
            layer3,
            layer4,
            output_layer,
            n_classes: self.n_classes,
            num_features: self.num_features,
        }
    }

    // pub fn init_instance<B: Backend>(&self, device: &B::Device) -> ClassModel3<B> {
    //     let layer1 = InstanceNormConfig::new(self.num_features);

    // }
}

pub trait CusForward<B: Backend> {
    fn forward(&self, item: ClassDataBatch<B>) -> ClassificationOutput<B>; 
}

impl<B: Backend> CusForward<B> for ClassModel<B> {
    fn forward(&self, item: ClassDataBatch<B>) -> ClassificationOutput<B> {
        let [batch_size, seq_len] = item.inputs.dims();
        let mut x = item.inputs.unsqueeze_dim(0);
        let (x, state) = self.transformer.forward(x, None);
        let (x, state) = self.input_layer1.forward(x, None);
        // let (x, state) = self.input_layer1.forward(x, Some(state));
        let output = self.output_layer.forward(x);
        let output = output.slice([0..1, 0..batch_size,  0..3]).reshape([batch_size, self.n_classes]);
        let output_classification = softmax(output, 1);
        let loss = CrossEntropyLossConfig::new()
            .init(&output_classification.device())
            .forward(output_classification.clone(), item.labels.clone());
        // loge!("ctp", "{:?}", output_classification);
        ClassificationOutput {
            loss,
            output: output_classification,
            targets: item.labels.clone(),
        }
    }
}

impl<B: Backend> CusForward<B> for ClassModel2<B> {
    fn forward(&self, item: ClassDataBatch<B>) -> ClassificationOutput<B> {
        // let mut x = item.inputs.unsqueeze_dim(0);
        let mut x = self.layer1.forward(item.inputs);
        x = self.layer2.forward(x);
        x = self.layer3.forward(x);
        x = self.layer4.forward(x);
        x = self.output_layer.forward(x);
        let output_classification = softmax(x, 1);
        // loge!("ctp", "{:?}", output_classification);
        let loss = CrossEntropyLossConfig::new()
            .init(&output_classification.device())
            .forward(output_classification.clone(), item.labels.clone());
        // loge!("ctp", "{:?}", output_classification);
        ClassificationOutput {
            loss,
            output: output_classification,
            targets: item.labels.clone(),
        }
    }
}


impl<B: AutodiffBackend> TrainStep<ClassDataBatch<B>, ClassificationOutput<B>> for ClassModel<B> {
    fn step(&self, item: ClassDataBatch<B>) -> TrainOutput<ClassificationOutput<B>> {
        let item = self.forward(item);
        let grads = item.loss.backward();
        TrainOutput::new(self, grads, item)
    }
}

impl<B: Backend> ValidStep<ClassDataBatch<B>, ClassificationOutput<B>> for ClassModel<B> {
    fn step(&self, item: ClassDataBatch<B>) -> ClassificationOutput<B> {
        self.forward(item)
    }
}

impl<B: AutodiffBackend> TrainStep<ClassDataBatch<B>, ClassificationOutput<B>> for ClassModel2<B> {
    fn step(&self, item: ClassDataBatch<B>) -> TrainOutput<ClassificationOutput<B>> {
        let item = self.forward(item);
        let grads = item.loss.backward();
        TrainOutput::new(self, grads, item)
    }
}

impl<B: Backend> ValidStep<ClassDataBatch<B>, ClassificationOutput<B>> for ClassModel2<B> {
    fn step(&self, item: ClassDataBatch<B>) -> ClassificationOutput<B> {
        self.forward(item)
    }
}

