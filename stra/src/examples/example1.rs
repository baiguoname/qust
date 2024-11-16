use serde::{ Serialize, Deserialize };
use qust::prelude::*;

use super::example5;
/*aaaa
let dil = gen_di.get(rl5m.clone());
//dil是获取数据，rl5m指的是5min k线，可以获取哪些k线可以看kline链接
//let dil = gen_di.get(rl10m.clone());
//let dil = gen_di.get((rl5m.clone(), vec![aler, zner]));
let ptm = example5::get_two_ma_ptm(10, 20);
//ptm就是一套规则，包括开平仓、资金等，这里是双均线的开平规则，10表示短线参数，20表示长线参数
//let ptm = example5::get_two_ma_ptm(20, 50);
let pnl_vec = ptm.dil(&dil).calc(cs2.clone());
//这里是计算pnl，ptm.dil(&dil)表示将那套规则用到dil数据上，cs2表示设定的手续费和滑点
let string_res = dil;
//string_res表示要输出的信息，这里输出数据的信息
//可以是其他可以转成字符串的变量，比如rl5m.clone().debug_string(), 表示数据5mk线合成方式的信息
let plot_res = pnl_vec.sum().plot();
//plot_res是要输出的图表
//let plot_res = pnl_vec.aplot(8);
let csv_res = pnl_vec.sum();
//csv_res表示要输出的表格信息, 如果超过3000行会被截到最后3000行
//let csv_res = dil.dil[0].pcon.price.clone();//输出单个数据的表
//let csv_res = dil.dil[0].pnl(&ptm, &cs2.clone());//输出单个pnl的表
aaaa*/