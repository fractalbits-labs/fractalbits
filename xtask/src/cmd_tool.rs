use crate::*;
use cmd_lib::*;
use comfy_table::{Table, presets};
use std::collections::HashMap;

pub fn run_cmd_tool(tool_kind: ToolKind) -> CmdResult {
    match tool_kind {
        ToolKind::GenUuids { num, file } => {
            xtask_common::gen_uuids(num, &file)?;
        }
        ToolKind::DescribeStack { stack_name } => {
            describe_stack(&stack_name)?;
        }
        ToolKind::DumpVgConfig { localdev } => {
            xtask_common::dump_vg_config(localdev)?;
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
