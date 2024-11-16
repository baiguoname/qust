use burn::backend::ndarray::NdArrayDevice;
use burn::backend::{Autodiff, NdArray};
use qust::prelude::{aler, jupyter_logging};
use stra::p03::run;



fn main() {
    // let guard = jupyter_logging(&[aler]);
    let device = NdArrayDevice::Cpu;
    run::run::<Autodiff<NdArray>>(device);

    run::predict();
}