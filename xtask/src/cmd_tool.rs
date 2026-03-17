use crate::*;
use cmd_lib::*;
use comfy_table::{Table, presets};
use std::collections::HashMap;

pub fn run_cmd_tool(tool_kind: ToolKind) -> CmdResult {
    match tool_kind {
        ToolKind::GenUuids { num, file } => {
            xtask_common::gen_uuids(num, &file)?;
        }
        ToolKind::DescribeStack {
            stack_name,
            gcp,
            gcp_project,
            gcp_zone,
        } => {
            if gcp {
                describe_gcp_stack(gcp_project.as_deref(), gcp_zone.as_deref())?;
            } else {
                describe_stack(&stack_name)?;
            }
        }
        ToolKind::DumpVgConfig { localdev } => {
            xtask_common::dump_vg_config(localdev)?;
        }
        ToolKind::SourceFile {
            core_sha,
            file_hash,
        } => {
            source_file(core_sha.as_deref(), file_hash.as_deref())?;
        }
    }
    Ok(())
}

fn describe_stack(stack_name: &str) -> CmdResult {
    // Get direct EC2 instance IDs from the CloudFormation stack
    let direct_instance_ids = match run_fun! {
        aws cloudformation describe-stack-resources
            --stack-name "$stack_name"
            --query r#"StackResources[?ResourceType==`AWS::EC2::Instance`].PhysicalResourceId"#
            --output text 2>/dev/null
    } {
        Ok(output) => output,
        Err(_) => {
            warn!("Stack '{stack_name}' does not exist or is not accessible");
            warn!(
                "Please deploy the stack first using 'just deploy' or check if you're using the correct AWS credentials"
            );
            return Ok(());
        }
    };

    // Get Auto Scaling Group names from the CloudFormation stack
    let asg_names = run_fun! {
        aws cloudformation describe-stack-resources
            --stack-name "$stack_name"
            --query r#"StackResources[?ResourceType==`AWS::AutoScaling::AutoScalingGroup`].PhysicalResourceId"#
            --output text
    }?;

    // Collect all ASG instance IDs
    let mut asg_instance_ids = Vec::new();
    if !asg_names.trim().is_empty() {
        for asg_name in asg_names.split_whitespace() {
            let asg_instances = run_fun! {
                aws autoscaling describe-auto-scaling-groups
                    --auto-scaling-group-names "$asg_name"
                    --query r#"AutoScalingGroups[].Instances[].InstanceId"#
                    --output text
            }?;

            if !asg_instances.trim().is_empty() {
                asg_instance_ids.extend(asg_instances.split_whitespace().map(|s| s.to_string()));
            }
        }
    }

    // Get instances by CloudFormation stack tag
    let tagged_instance_ids = run_fun! {
        aws ec2 describe-instances
            --filters "Name=tag:aws:cloudformation:stack-name,Values=$stack_name"
                      "Name=instance-state-name,Values=pending,running,stopping,stopped"
            --query r#"Reservations[].Instances[].InstanceId"#
            --output text
    }?;

    // Get instances by Name tag prefix
    let name_prefix_instance_ids = run_fun! {
        aws ec2 describe-instances
            --filters "Name=tag:Name,Values=${stack_name}/*"
                      "Name=instance-state-name,Values=pending,running,stopping,stopped"
            --query r#"Reservations[].Instances[].InstanceId"#
            --output text
    }?;

    // Combine all instance IDs and remove duplicates
    let mut all_instance_ids = Vec::new();

    // Add direct instance IDs
    all_instance_ids.extend(
        direct_instance_ids
            .split_whitespace()
            .map(|s| s.to_string()),
    );

    // Add ASG instance IDs
    all_instance_ids.extend(asg_instance_ids);

    // Add tagged instance IDs
    all_instance_ids.extend(
        tagged_instance_ids
            .split_whitespace()
            .map(|s| s.to_string()),
    );

    // Add name prefix instance IDs
    all_instance_ids.extend(
        name_prefix_instance_ids
            .split_whitespace()
            .map(|s| s.to_string()),
    );

    // Remove duplicates and empty strings
    all_instance_ids.sort();
    all_instance_ids.dedup();
    all_instance_ids.retain(|id| !id.is_empty());

    if all_instance_ids.is_empty() {
        warn!("No EC2 instances found in stack: {}", stack_name);
        return Ok(());
    }

    // Get NLB ARNs from the CloudFormation stack
    let nlb_arns = run_fun! {
        aws cloudformation describe-stack-resources
            --stack-name "$stack_name"
            --query r#"StackResources[?ResourceType==`AWS::ElasticLoadBalancingV2::LoadBalancer`].PhysicalResourceId"#
            --output text
    }?;

    // Get NLB DNS names
    let mut nlb_endpoint = String::new();
    if !nlb_arns.trim().is_empty() {
        for nlb_arn in nlb_arns.split_whitespace() {
            let nlb_dns = run_fun! {
                aws elbv2 describe-load-balancers
                    --load-balancer-arns "$nlb_arn"
                    --query r#"LoadBalancers[0].DNSName"#
                    --output text
            }?;

            if !nlb_dns.trim().is_empty() {
                nlb_endpoint = nlb_dns.trim().to_string();
                break;
            }
        }
    }

    // Get zone name to zone ID mapping
    let zone_info = run_fun! {
        aws ec2 describe-availability-zones
            --query r#"AvailabilityZones[].[ZoneName,ZoneId]"#
            --output text
    }?;

    let mut zone_map = HashMap::new();
    for line in zone_info.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() == 2 {
            zone_map.insert(parts[0].to_string(), parts[1].to_string());
        }
    }

    // Get instance details
    let instance_details = run_fun! {
        aws ec2 describe-instances
            --instance-ids $[all_instance_ids]
            --query r#"Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,InstanceId,State.Name,InstanceType,Placement.AvailabilityZone,PrivateIpAddress]"#
            --output text
    }?;

    // Collect instance data for sorting
    let mut instances: Vec<(String, String, String, String, String, String, String)> = Vec::new();
    for line in instance_details.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 6 {
            let name = if parts[0] == "None" { "" } else { parts[0] };
            let instance_id = parts[1];
            let state = parts[2];
            let instance_type = parts[3];
            let az = parts[4];
            let private_ip = if parts[5] == "None" { "-" } else { parts[5] };
            let zone_id = zone_map.get(az).map(|s| s.as_str()).unwrap_or("N/A");

            instances.push((
                name.to_string(),
                instance_id.to_string(),
                state.to_string(),
                instance_type.to_string(),
                az.to_string(),
                zone_id.to_string(),
                private_ip.to_string(),
            ));
        }
    }

    // Sort by name (first column)
    instances.sort_by(|a, b| a.0.cmp(&b.0));

    // Add (1/N, 2/N, ...) suffixes for ASG instances with the same name
    let mut name_counts: HashMap<String, usize> = HashMap::new();
    for (name, _, _, _, _, _, _) in &instances {
        *name_counts.entry(name.clone()).or_insert(0) += 1;
    }

    let mut name_indices: HashMap<String, usize> = HashMap::new();
    for (name, _, _, _, _, _, _) in &mut instances {
        if let Some(&count) = name_counts.get(name)
            && count > 1
        {
            let idx = name_indices.entry(name.clone()).or_insert(0);
            *idx += 1;
            *name = format!("{} ({}/{})", name, idx, count);
        }
    }

    // Create and populate the table
    let mut table = Table::new();
    table.load_preset(presets::NOTHING);
    table.set_header(vec![
        "Name",
        "InstanceId",
        "State",
        "InstanceType",
        "AvailabilityZone",
        "ZoneId",
        "PrivateIP",
    ]);

    for (name, instance_id, state, instance_type, az, zone_id, private_ip) in instances {
        table.add_row(vec![
            name,
            instance_id,
            state,
            instance_type,
            az,
            zone_id,
            private_ip,
        ]);
    }

    println!("{table}");

    if !nlb_endpoint.is_empty() {
        println!("\n API Server NLB Endpoint: {nlb_endpoint}");
    }

    Ok(())
}

fn resolve_gcp_project(cli_arg: Option<&str>) -> Result<String, std::io::Error> {
    if let Some(p) = cli_arg.filter(|s| !s.is_empty()) {
        return Ok(p.to_string());
    }
    if let Ok(p) = std::env::var("GCP_PROJECT_ID")
        && !p.is_empty()
    {
        return Ok(p);
    }
    // Fall back to gcloud config default project
    if let Ok(p) = run_fun!(gcloud config get-value project 2>/dev/null)
        && !p.trim().is_empty()
    {
        return Ok(p.trim().to_string());
    }
    Err(std::io::Error::other(
        "GCP project ID required. Set via --gcp-project, GCP_PROJECT_ID env var, or gcloud config",
    ))
}

fn resolve_gcp_zone(cli_arg: Option<&str>) -> String {
    if let Some(z) = cli_arg.filter(|s| !s.is_empty()) {
        return z.to_string();
    }
    std::env::var("GCP_ZONE").unwrap_or_else(|_| "us-central1-a".to_string())
}

fn describe_gcp_stack(gcp_project: Option<&str>, gcp_zone: Option<&str>) -> CmdResult {
    let project_id = resolve_gcp_project(gcp_project)?;
    let zone = resolve_gcp_zone(gcp_zone);
    let region = zone
        .rsplit_once('-')
        .map(|(r, _)| r)
        .unwrap_or("us-central1");

    // List all instances in the project with fractalbits network tags
    let instance_list = match run_fun! {
        gcloud compute instances list
            --project $project_id
            --format "csv[no-heading](name,zone.basename(),status,machineType.basename(),networkInterfaces[0].networkIP,metadata.items.filter(key:service-role).extract(value).flatten())"
            --filter "networkInterfaces.network:fractalbits-vpc"
    } {
        Ok(output) => output,
        Err(_) => {
            warn!("No GCP instances found in project '{project_id}' with fractalbits VPC");
            warn!(
                "Please deploy first using 'just deploy create-vpc --deploy-target gcp' or check GCP credentials"
            );
            return Ok(());
        }
    };

    if instance_list.trim().is_empty() {
        warn!("No GCP instances found in project '{project_id}' with fractalbits VPC");
        return Ok(());
    }

    // Parse and collect instance data
    let mut instances: Vec<(String, String, String, String, String, String)> = Vec::new();
    for line in instance_list.lines() {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 5 {
            let name = parts[0];
            let inst_zone = parts[1];
            let status = parts[2];
            let machine_type = parts[3];
            let private_ip = if parts[4].is_empty() { "-" } else { parts[4] };
            let role = if parts.len() >= 6 && !parts[5].is_empty() {
                parts[5]
            } else {
                "-"
            };
            instances.push((
                name.to_string(),
                inst_zone.to_string(),
                status.to_string(),
                machine_type.to_string(),
                private_ip.to_string(),
                role.to_string(),
            ));
        }
    }

    // Sort by name
    instances.sort_by(|a, b| a.0.cmp(&b.0));

    // Create and populate the table
    let mut table = Table::new();
    table.load_preset(presets::NOTHING);
    table.set_header(vec![
        "Name",
        "Zone",
        "Status",
        "MachineType",
        "PrivateIP",
        "Role",
    ]);

    for (name, inst_zone, status, machine_type, private_ip, role) in &instances {
        table.add_row(vec![
            name,
            inst_zone,
            status,
            machine_type,
            private_ip,
            role,
        ]);
    }

    println!("{table}");

    // Get internal LB endpoint
    let lb_ip = run_fun! {
        gcloud compute forwarding-rules list
            --project $project_id
            --regions $region
            --format "csv[no-heading](IPAddress)"
            --filter "name:api-lb"
    };
    if let Ok(ip) = lb_ip
        && !ip.trim().is_empty()
    {
        println!("\n API Server LB Endpoint: {}", ip.trim());
    }

    Ok(())
}

fn source_file(core_sha: Option<&str>, file_hash: Option<&str>) -> CmdResult {
    let core_path = crate::ZIG_REPO_PATH;
    let sha = match core_sha {
        Some(s) => s.to_string(),
        None => run_fun!(git -C $core_path rev-parse HEAD)?,
    };

    let file_list = run_fun!(git -C $core_path ls-tree -r --name-only $sha)?;
    let mut mappings: Vec<(String, String)> = Vec::new();

    for path in file_list.lines() {
        if !path.ends_with(".zig") {
            continue;
        }
        let hash = xxhash_rust::xxh3::xxh3_64(path.as_bytes());
        mappings.push((format!("{hash:x}"), path.to_string()));
    }

    mappings.sort_by(|a, b| a.1.cmp(&b.1));

    match file_hash {
        Some(needle) => {
            let mut found = false;
            for (hash, path) in &mappings {
                if hash == needle {
                    println!("{hash} => {path}");
                    found = true;
                }
            }
            if !found {
                warn!("No file found matching hash: {needle}");
            }
        }
        None => {
            for (hash, path) in &mappings {
                println!("{hash} => {path}");
            }
        }
    }

    Ok(())
}
