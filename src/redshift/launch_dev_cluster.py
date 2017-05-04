import redshift_provision

##Note: define your AWS credentials before running (for example, in boto run: AWS CONFIGURE)

##Variables
#Cluster details
source_cluster = "floactionmcm"
new_cluster_code = "-dev-"
old_cluster_prefix = source_cluster + new_cluster_code

#CNAME Flip variables
hosted_zone_id="Z2I3829AKAAKDKMK"
resource_record_set_name = "test.example.com"

#Cluster Resize Options
ResizeNodeType="ds2.xlarge"
ResizeNodeNumber=1


##Begin Script
#Validate the source_cluster variable to make sure that cluster_id exists in the AWS account.  This is needed because the source_cluster is tied to the snapshot from which the new dev cluster will be restored.
redshiftprovision.RedshiftCluster(source_cluster).validate_cluster_exists()

#Get the existing dev cluster ID based on the prefix in the old_cluster_prefix variable
old_cluster = redshiftprovision.RedshiftCluster(old_cluster_prefix).get_cluster_containing()

#Get latest cluster snapshot
snapshot = redshiftprovision.RedshiftCluster(source_cluster).get_latest_snapshot()

#Build the new cluster name
new_cluster_id = redshiftprovision.RedshiftCluster(source_cluster).generate_new_cluster_id(new_cluster_code)

#Restore the new cluster
restored_cluster =  redshiftprovision.RedshiftCluster(source_cluster).create_cluster_from_snapshot(snapshot, new_cluster_id)

#Resize cluster
redshiftprovision.RedshiftCluster(restored_cluster).resize_cluster(ResizeNodeType,ResizeNodeNumber)

#Flip CNAME
redshiftprovision.Route53RecordSet(hosted_zone_id).upsert_cname_record_set(redshiftprovision.RedshiftCluster(restored_cluster).get_cluster_hostname(),resource_record_set_name)

#Delete old cluster
deleted_cluster = redshiftprovision.RedshiftCluster(old_cluster).delete_cluster()
