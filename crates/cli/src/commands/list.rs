//! List subcommand implementation.

use s3f::{ListRequest, S3Client};

use crate::{cli::ListArgs, fmt};

/// Execute the ls subcommand.
///
/// Listing runs as concurrent key ranges, so pages arrive out of order;
/// results are collected and printed sorted by key.
pub(crate) async fn run(client: &S3Client, args: &ListArgs, quiet: bool) -> anyhow::Result<()> {
    let req = ListRequest::new(&args.bucket, &args.prefix).with_concurrency(args.workers);
    let objects = client.list(req).collect_all().await?;

    let total_objects = objects.len();
    let total_size: u64 = objects.iter().map(|o| o.size).sum();

    if !quiet {
        for obj in &objects {
            let date = obj.last_modified.get(..10).unwrap_or(&obj.last_modified);
            println!("{:>10}  {}  {}", fmt::bytes(obj.size), date, obj.key);
        }
    }

    if !quiet {
        println!();
        println!("{} object(s), {} total", total_objects, fmt::bytes(total_size));
    }

    Ok(())
}
