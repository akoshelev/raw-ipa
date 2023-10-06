// use std::thread::Thread;
// use lazy_static::lazy_static;
// use thiserror::Error;
//
//
// lazy_static! {
//     pub static ref RUNTIME: IpaRuntime = IpaRuntime::try_new().unwrap();
// }
//
//
// /// Runtimes per core
// struct IpaRuntime {
//     // runtimes: Vec<tokio::runtime::Runtime>
//     thread_pool: Vec<Thread>
// }
//
// #[derive(Error)]
// enum RuntimeError {
//     #[error("Failed to get core ids")]
//     CoreIdsUnavailable,
// }
//
// impl IpaRuntime {
//     fn try_new() -> Result<Self, RuntimeError> {
//         let cores = core_affinity::get_core_ids().ok_or(RuntimeError::CoreIdsUnavailable)?;
//         let thread_pool = cores.into_iter().map(|core| {
//             std::thread::spawn()
//         }).collect();
//
//         // let mut runtimes = Vec::with_capacity(cores.len());
//         // let runtimes = cores.into_iter().map(|core| {
//         //     tokio::runtime::Builder::new_current_thread()
//         //         .enable_all()
//         //         .worker_threads(1)
//         //         .thread_name(format!("ipa-{}", core))
//         //         .build()
//         //         .unwrap()
//         // }).collect();
//         // }
//     }
// }