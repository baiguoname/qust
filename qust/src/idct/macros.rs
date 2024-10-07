#[macro_export]
macro_rules! msig {
    ($func: expr, $cond1: expr, $cond2: expr) => {{
        let cond1_box: Box<dyn Cond> = Box::new($cond1.clone());
        let cond2_box: Box<dyn Cond> = Box::new($cond2.clone());
        MsigType($func, cond1_box, cond2_box)
    }};
    ($func: expr, $cond1: expr, $cond2: expr, $cond3: expr) => {
        $crate::msig!($func, $crate::msig!($func, $cond1, $cond2), $cond3)
    };
    ($func: expr, $cond1: expr, $cond2: expr, $cond3: expr, $cond4: expr) => {
        $crate::msig!($func, $crate::msig!($func, $cond1, $cond2, $cond3), $cond4)
    };
}

#[macro_export]
macro_rules! dcon {
    ($c1: expr, $c2: expr) => {
        PreNow(Box::new($c1.clone()), Box::new($c2.clone()))
    };
    ($c1: expr, $c2: expr, $c3: expr) => {
        $crate::dcon!($crate::dcon!($c1, $c2), $c3)
    };
    ($c1: expr, $c2: expr, $c3: expr, $c4: expr) => {
        $crate::dcon!($crate::dcon!($c1, $c2, $c3), $c4)
    };
}

#[macro_export]
macro_rules! fore_ta {
    ($ta1: expr, $ta2: expr) => {
        ForeTa(Box::new($ta1.clone()), Box::new($ta2.clone()))
    };
    ($ta1: expr, $ta2: expr, $ta3: expr) => {
        $crate::fore_ta!($crate::fore_ta!($ta1, $ta2), $ta3)
    };
}

#[macro_export]
macro_rules! pms {
    ($dcon: expr, $part: expr, $fore: expr) => {{
        let ta_box: Box<dyn Ta> = Box::new($fore.clone());
        PmsType {
            dcon: $dcon.clone(),
            part: $part.clone(),
            fore: ta_box,
        }
    }};
    ($dcon: expr, $part: expr, $fore1: expr, $fore2: expr) => {
        $crate::pms!($dcon, $part, $crate::fore_ta!($fore1, $fore2))
    };
    ($dcon: expr, $part: expr, $fore1: expr, $fore2: expr, $fore3: expr) => {
        pms!($dcon, $part, $crate::fore_ta!($fore1, $fore2, $fore3))
    };
}

#[macro_export]
macro_rules! gen_inter {
    ($inter: ident, $vals: expr, $box: ident) => {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct $inter;

        #[typetag::serde]
        impl Inter for $inter {
            fn intervals(&self) -> Vec<Interval> {
                $vals
            }
        }

        lazy_static! {
            pub static ref $box: InterBox = Box::new($inter);
        }
    };
}

#[macro_export]
macro_rules! loge {
    ($target: expr, $($args: tt)*) => {
        loge!(level: Info, $target, $($args)*)
    };
    (level: $level: tt, $target: expr, $($args: tt)*) => {
        log::logger().log(
            &log::Record::builder()
                .args(format_args!($($args)*))
                .level(log::Level::$level)
                .target(&format!("{}", $target))
                .line(Some(line!()))
                .file(Some(file!()))
                .build()
        )
    }
}