use qust::prelude::*;
use chrono::Duration;


gen_inter!(
    Rl1mNight,
    [
        even_slice_time(210000.to_tt(), 230000.to_tt(), Duration::seconds(60), Duration::milliseconds(500)),
    ].concat(),
    rl1m_night
);

gen_inter!(
    Rl5mAll3,
    [
        even_slice_time_usize(210230, 225930, 300),
        even_slice_time_usize(230030, 235430, 300),
        vec![Interval::Time(235430.5.to_tt(), 235959.5.to_tt())],
        even_slice_time_usize(30, 15930, 300),
        rl5m.intervals(),
    ].concat(),
    rl5mall3
);

gen_inter!(
    Rl30sDay,
    even_slice_time_usize(90000, 145959, 30),
    rl30sday
);

gen_inter!(
    Rl30sAll,
    [
        even_slice_time_usize(90000, 145959, 30),
        even_slice_time_usize(210000, 225900, 30),
    ].concat(),
    rl30sall
);

gen_inter!(
    Rl1mDay,
    even_slice_time_usize(90000, 145959, 60),
    rl1mday
);

gen_inter!(
    Rl2mDay, 
    even_slice_time_usize(90000, 145959, 120),
    rl2mday
);

gen_inter!(
    Rl1mAll, 
    [
        even_slice_time_usize(90000, 145940, 60), 
        even_slice_time_usize(210000, 225900, 60),
        ]
        .concat(),
    rl1mall
);

gen_inter!(
    Rl2mAll, 
    [
        even_slice_time_usize(90000, 145940, 120), 
        even_slice_time_usize(210000, 225900, 120),
        ]
        .concat(),
    rl2mall
);

gen_inter!(
    Rl1mAll2, 
    [   
        even_slice_time_usize(0, 30000, 60),
        even_slice_time_usize(90000, 145940, 60), 
        even_slice_time_usize(210000, 225900, 60),
        ]
        .concat(),
    rl1mall2
);

gen_inter!(
    Rl5mAll, 
    [   
        even_slice_time_usize(0, 30000, 300),
        even_slice_time_usize(90000, 145940, 300), 
        even_slice_time_usize(210000, 235950, 300),
        ]
        .concat(),
    rl5mall
);

gen_inter!(
    Rl30mDayBare,
    [
        even_slice_time_usize(90000, 101455, 1800),
        even_slice_time_usize(103000, 112955, 1800),
        even_slice_time_usize(133000, 145955, 1800),
    ]
        .concat(),
    rl30m_day_bare
);

gen_inter!(
    Rl30mAllBare,
    [
        Rl30mDayBare.intervals(),
        even_slice_time_usize(210000, 235955, 1800),
        even_slice_time_usize(0, 20000, 1800),
    ]
        .concat(),
    rl30m_all_bare
);

gen_inter!(
    Rln30s,
    even_slice_time_usize(00000, 235900, 30),
    rln30s
);

gen_inter!(
    Rln30m,
    [
        Rl30mDay.intervals(),
        even_slice_time_usize(210000, 225930, 1800),
    ]
        .concat(),
    rln30m
);

gen_inter!(
    Rln10s,
    even_slice_time_usize(90000, 235900, 10),
    rln10s
);

gen_inter!(
    Rl10m,
    [
        even_slice_time_usize(90000, 101455, 600),
        even_slice_time_usize(103000, 112955, 600),
        even_slice_time_usize(133000, 145955, 600),
    ]
        .concat(),
    rl10m
);
