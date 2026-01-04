pub mod data_blob_resyncing;
pub mod data_blob_tracking;

use crate::{
    CmdResult, InitConfig, MultiAzTestType, ServiceName, cmd_build::BuildMode, cmd_service,
};

pub async fn run_multi_az_tests(test_type: MultiAzTestType) -> CmdResult {
    let setup_multi_az = || async {
        cmd_service::init_service(
            ServiceName::All,
            BuildMode::Debug,
            InitConfig {
                data_blob_storage: crate::DataBlobStorage::S3ExpressMultiAz,
                bss_count: 0, // No BSS services in multi-AZ mode
                ..Default::default()
            },
        )?;
        cmd_service::start_service(ServiceName::All)
    };
    let test_tracking = || async {
        setup_multi_az().await?;
        data_blob_tracking::run_multi_az_tests().await?;
        cmd_service::stop_service(ServiceName::All)
    };
    let test_resyncing = || async {
        setup_multi_az().await?;
        data_blob_resyncing::run_data_blob_resyncing_tests().await?;
        cmd_service::stop_service(ServiceName::All)
    };
    match test_type {
        MultiAzTestType::DataBlobTracking => test_tracking().await?,
        MultiAzTestType::DataBlobResyncing => test_resyncing().await?,
        MultiAzTestType::All => {
            test_tracking().await?;
            test_resyncing().await?;
        }
    }
    Ok(())
}
