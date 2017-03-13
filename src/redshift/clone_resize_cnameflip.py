import sys, traceback
import boto3
from datetime import datetime, timedelta
import time

#Overview
#This script is intended to periodically refresh a dev or test cluster from a prod cluster's snapshot.

#What it does
#Creates a new cloned cluster based on the latest snapshot of the cluster defined
#in the ClusterIdentifierOrigin variable, resizes it, and deletes the old version of the old cloned cluster.
#Future add is to flip the CNAME record set to point to the new cloned cluster

##User Defined Variables##
#Cluster that is the source of the snapshot for the dev cluster
ClusterIdentifierOrigin = "floactionmcm"
#Dev cluster identifier prefix.  A timestamp is added to this string when the cluster is restored from the snapshot.
ClusterIdentifierPrefixDev = "floactionmcm-dev"

#Debugging options
RestoreFlag=True
ResizeFlag=False
DeleteOldClusterFlag=True

#Cluster resize options
ResizeNodeType="ds2.xlarge"
ResizeNodeNumber=2

##Begin Script##
client = boto3.client('redshift')

#Generate the current timestamp as a string to use as the cluster identifier suffix
today = datetime.today()

version = str(today.year) + "-" + str(today.month) + "-" + str(today.day)+ "-" + str(today.hour) + "-" + str(today.minute)

#Validate the cluster identifier that is the source of the snapshot to be restored
#TODO: Create a function that also returns the cluster status
response = client.describe_clusters(
    ClusterIdentifier=ClusterIdentifierOrigin
)

try:
    ClusterIdentifier = response['Clusters'][0]['ClusterIdentifier']
except KeyError:
    traceback.print_exc()
    sys.exit(3)

print("New cluster will be cloned from: " + ClusterIdentifier)

#Look for snapshots with a snapshot on or before yesterday
SnapshotStartDate = today + timedelta(days=-1)

print("Look for snapshots created after: " + str(SnapshotStartDate))

#Get the latest snapshot for the originating cluster
response = client.describe_cluster_snapshots(
    ClusterIdentifier=ClusterIdentifier,
    SnapshotType='automated'
)

try:
    SnapshotIdentifier =  response['Snapshots'][0]['SnapshotIdentifier']
except KeyError:
    traceback.print_exc()
    sys.exit(3)

print("The new cluster will be restored from the snapshot identifier: " + str(SnapshotIdentifier))

#Get cluster identifer of old cluster to be replaced by new cluster
response = client.describe_clusters()

#Get the 'Clusters' lists from the dictionary
clusters = response['Clusters']

try:
    #From the dictionaries within the 'Clusters' lists, get the ClusterIdentifier values
    OldDevClusterCount=0
    for i in clusters:
        if ClusterIdentifierPrefixDev in i['ClusterIdentifier'] and i['ClusterStatus']=="available":
            OldDevClusterCount += 1
            ClusterIdentifierOldDev = i['ClusterIdentifier']
            print("Old dev cluster: " + ClusterIdentifierOldDev)
        else:
            ClusterIdentifierOldDev = None
    print("Old dev cluster OldDevClusterCount: " + str(OldDevClusterCount))
except KeyError:
    traceback.print_exc()
    sys.exit(3)

#For the new cluster name, append the datetime string to the end of the originating cluster name
ClusterIdentifierNew = ClusterIdentifierPrefixDev + version

print("The new cluster identifier will be: " + ClusterIdentifierNew)

#Restore the cluster from the snapshot
if RestoreFlag==True:
    print("Restoring the cluster from snapshot: " + str(SnapshotIdentifier))
    response = client.restore_from_cluster_snapshot(
        ClusterIdentifier=ClusterIdentifierNew,
        SnapshotIdentifier=SnapshotIdentifier,
        SnapshotClusterIdentifier=ClusterIdentifier
    )

    try:
        ClusterIdentifierNew2 = response['Cluster']['ClusterIdentifier']
    except KeyError:
        traceback.print_exc()
        sys.exit(3)
    print("Restoring has started for new cluster identifier: " + ClusterIdentifierNew2)

    #Wait for the new cluster to become available
    print("Waiting for new cluster to be available...")

    waiter = client.get_waiter('cluster_available')

    try:
        waiter.wait(
        ClusterIdentifier=ClusterIdentifierNew2,
        MaxRecords=100
    )
    except Timeout:
        print("The restore wait operation has timed out.  Check the console for completion status")
        traceback.print_exc()
        sys.exit(3)

    print("Cluster Available")

    #Wait for cluster restore to be fully complete
    print("Waiting for cluster restore status to be completed...")
    ClusterRestoreStatus = ""
    ClusterRestoreStatusFinal = "completed"
    while (ClusterRestoreStatus!=ClusterRestoreStatusFinal):
        response = client.describe_clusters(
            ClusterIdentifier=ClusterIdentifierNew2
    )
        try:
            ClusterRestoreStatus = response['Clusters'][0]['RestoreStatus']['Status']
            if ClusterRestoreStatus != ClusterRestoreStatusFinal:
                print("Cluster restore status is: " + ClusterRestoreStatus + ".  Waiting 30 seconds for completed status" )
                time.sleep(30)
            else:
                print("Cluster restore status is: " + ClusterRestoreStatus)
        except KeyError:
            traceback.print_exc()
            sys.exit(3)
    print("Cluster restore has completed.")
else:
    print("Skipping cluster restore.")

#Resize the cluster
#TODO: Check the space utilization on the originating cluster and resize based on that.  Need a function to check the size.
if ResizeFlag == True:
    print("Starting Resize")
    response = client.modify_cluster(
        ClusterIdentifier=ClusterIdentifierNew,
        ClusterType='multi-node',#TODO: make this dynamic based on ResizeNodeNumber
        NodeType=ResizeNodeType,
        NumberOfNodes=ResizeNodeNumber
    )

    #Wait for cluster restore to be fully complete
    print("Waiting for cluster resize to be completed...")
    ClusterResizeStatus = ""
    ClusterResizeStatusFinal = "available"
    while (ClusterResizeStatus!=ClusterResizeStatusFinal):
        response = client.describe_clusters(
            ClusterIdentifier=ClusterIdentifierNew2
    )
        try:
            ClusterResizeStatus = response['Clusters'][0]['ClusterStatus']
            if ClusterResizeStatus !=ClusterResizeStatusFinal:
                print("Cluster status is : " + ClusterResizeStatus + ".  Waiting 30 seconds for available status")
                time.sleep(30)
            else:
                print("Cluster status is : " + ClusterResizeStatus )
        except KeyError:
            traceback.print_exc()
            sys.exit(3)
    print("Cluster resize complete.")
else:
    print("Skipping resize")

#TODO update CNAME record http://boto3.readthedocs.io/en/latest/reference/services/route53.html#Route53.Client.change_resource_record_sets

#Delete old cluster
#TODO: Should wait for open sessions/queries to complete before deleting?
if DeleteOldClusterFlag == True:
    print("Starting old cluster deletion")
    if ClusterIdentifierOldDev != None and OldDevClusterCount == 1:
        print("Deleting old cluster: " + ClusterIdentifierOldDev)
        response = client.delete_cluster(
            ClusterIdentifier=ClusterIdentifierOldDev,
            SkipFinalClusterSnapshot=True
        )
    elif ClusterIdentifierOldDev != None and OldDevClusterCount > 1:
        print("Multiple old clusters exist.  None deleted.  Delete the old cluster(s) manually if they are no longer needed.")
    else:
        print("No old cluster to delete.")
else:
    print("Skipping old cluster deletion.")
