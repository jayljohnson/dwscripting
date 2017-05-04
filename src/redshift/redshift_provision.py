import sys, traceback
import boto3
from datetime import datetime, timedelta
import time
import logging

#Author: Jay L. Johnson, jay dot johnson at hotmail dot com
#Last Update: 2017-03-18

# create logger'
logger = logging.getLogger('redshiftprovision')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('redshiftprovision.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

class RedshiftCluster(str):
    global status_check_interval
    status_check_interval = 60

    def __init__(self, clusterid):
        self.clusterid = clusterid

    #Find existing clusters with the cluster identifier containing the functin parameter string
    def get_cluster_containing(self):
        client = boto3.client('redshift')

        #Get cluster identifer of old cluster to be replaced by new cluster
        response = client.describe_clusters()

        #Get the 'Clusters' lists from the dictionary
        clusters = response['Clusters']

        try:
            #From the dictionaries within the 'Clusters' lists, get the ClusterIdentifier values
            logging.info("Searching for cluster identifiers with ClusterStatus of Available, containing: " + self.clusterid)
            for i in clusters:
                if self.clusterid in i['ClusterIdentifier'] and i['ClusterStatus']=="available":
                    ClusterIdentifierOldDev = i['ClusterIdentifier']
                    logger.info("    Matching cluster found: " + ClusterIdentifierOldDev)
#                    logger.info(ClusterIdentifierOldDev)
                else:
                    #TODO: This is not working.
                    ClusterIdentifierOther = i['ClusterIdentifier']
                    ClusterIdentifierOldDev = None
                    logger.info("    Skipping clusterid: " + ClusterIdentifierOther)
            if ClusterIdentifierOldDev == None:
                logger.info("Cluster not found containing: " + self.clusterid)
                return None
            else:
                return ClusterIdentifierOldDev
                logger.info("Most recent Cluster Identifier matching cluster prefix: " + old_cluster_prefix + " is Cluster Identifier: " + ClusterIdentifierOldDev)
        except KeyError:
            logger.info("Unknown exception")
            traceback.print_exc()
            sys.exit(3)

    def validate_cluster_exists(self):
        client = boto3.client('redshift')

        try:
            response = client.describe_clusters(
                ClusterIdentifier=self.clusterid)
            ClusterIdentifier = response['Clusters'][0]['ClusterIdentifier']
            logger.info("Cluster Identifier is valid: " + ClusterIdentifier)
            return ClusterIdentifier
        #TODO: Exiting ungracefully with traceback but no logger.info() message
        except KeyError:
            traceback.print_exc()
            logger.info("Cluster Identifier does not exist: " + ClusterIdentifier)
            sys.exit(3)

    def get_cluster_hostname(self):
        client = boto3.client('redshift')

        try:
            response = client.describe_clusters(
                ClusterIdentifier=self.clusterid)
            if response <> None:
                ClusterHostName =  response['Clusters'][0]['Endpoint']['Address']
                logger.info("Cluster " + self.clusterid + " has hostname: " + ClusterHostName)
                return ClusterHostName
            else:
                logger.info("Unexpected result; can't find hostname")
                logger.info(response)
        except KeyError:
            traceback.print_exc()
            logger.info("Hostname not found.")
            sys.exit(3)

    def get_latest_snapshot(self):
        client = boto3.client('redshift')

        #Look for snapshots with a snapshot on or before yesterday
        today = datetime.today()
        SnapshotStartDate = today + timedelta(days=-1)

        logger.info("Searching for snapshots created after: " + str(SnapshotStartDate))

        #Get the latest snapshot for the originating cluster
        response = client.describe_cluster_snapshots(
           ClusterIdentifier=self.clusterid,
           SnapshotType='automated')

        try:
            SnapshotIdentifier =  response['Snapshots'][0]['SnapshotIdentifier']
            logger.info("    Snapshot found named: " + SnapshotIdentifier)
            return SnapshotIdentifier
        except KeyError:
            traceback.print_exc()
            sys.exit(3)

        logger.info("The most recent snapshot has snapshot identifier: " + str(SnapshotIdentifier))

    def generate_new_cluster_id(self, suffix):
        #For the new cluster name, append the datetime string to the end of the originating cluster name

        self.suffix = suffix

        #Set the datetime variables
        today = datetime.today()
        version = str(today.year) + "-" + str(today.month) + "-" + str(today.day)+ "-" + str(today.hour) + "-" + str(today.minute)

        #Build the new cluster identifier for the new cluster
        ClusterIdentifierNew = self.clusterid + self.suffix + version
        
        logger.info("Generating new cluster id")
        logger.info("    The new cluster identifier will be: " + ClusterIdentifierNew)
        return ClusterIdentifierNew

    def create_cluster_from_snapshot(self, snapshotidentifier, newclusterid):
        #For the new cluster name, append the datetime string to the end of the originating cluster name
        client = boto3.client('redshift')
        self.snapshotidentifier = snapshotidentifier
        self.newclusterid = newclusterid

        #Build the new cluster identifier for the new cluster
        ClusterIdentifierNew = self.newclusterid 

        #Restore the cluster from the snapshot
        logger.info("Restore starting for new cluster identifier: " + ClusterIdentifierNew)
        logger.info("    Restoring the cluster from snapshot: " + str(self.snapshotidentifier))
        response = client.restore_from_cluster_snapshot(
            ClusterIdentifier=ClusterIdentifierNew,
            SnapshotIdentifier=self.snapshotidentifier,
            SnapshotClusterIdentifier=self.clusterid)
        try:
            ClusterIdentifierNew2 = response['Cluster']['ClusterIdentifier']
        except KeyError:
            traceback.print_exc()
            sys.exit(3)
        logger.info("    Restoring is in progress for new cluster identifier: " + ClusterIdentifierNew2)

        #Wait for the new cluster to become available
        logger.info("    Waiting for new cluster to be available...")
        waiter = client.get_waiter('cluster_available')
        try:
            waiter.wait(ClusterIdentifier=ClusterIdentifierNew2,
                        MaxRecords=100)
        except Timeout:
            logger.info("The restore wait operation has timed out.  Check the console for completion status")
            traceback.print_exc()
            sys.exit(3)
        logger.info("    Cluster Available")

        #Wait for cluster restore to be fully complete
        logger.info("    Waiting for cluster restore status to be completed...")
        ClusterRestoreStatus = ""
        ClusterRestoreStatusFinal = "completed"

        while (ClusterRestoreStatus!=ClusterRestoreStatusFinal):
            response = client.describe_clusters(ClusterIdentifier=ClusterIdentifierNew2)
            try:
                ClusterRestoreStatus = response['Clusters'][0]['RestoreStatus']['Status']
                if ClusterRestoreStatus != ClusterRestoreStatusFinal:
                    logger.info("    Cluster restore status is: " + ClusterRestoreStatus + ".  Waiting " + str(status_check_interval) + " seconds for completed status" )
                    time.sleep(status_check_interval)
                else:
                    logger.info("    Cluster restore status is: " + ClusterRestoreStatus)
            except KeyError:
                traceback.print_exc()
                sys.exit(3)
        logger.info("Cluster restore has completed.")
        return ClusterIdentifierNew2

    def delete_cluster(self):
    #Delete old cluster
    #TODO: need handling for OldDevClusterCount.  This was originally in the get_cluster_containing logic.
    #TODO: Should wait for open sessions/queries to complete before deleting?  Also wait delay before CNAME flip, CNAME has 300 second TTL
    #TODO: Need to check for resizing completion.  Throws an error if resize is not complete.  Deleting old cluster: floactionmcm-dev-2017-3-19-0-16

		client = boto3.client('redshift')
      
        logger.info("Starting old cluster deletion.")
        #TODO need debugging here to see what self.clusterid contains
        if self.clusterid != None:
            logger.info("    Deleting old cluster in 300 seconds: " + self.clusterid)
            #TODO: not working correctly if more than one cluster with different statuses, needs testing. Also not working with 2+ clusters 
            time.sleep(300)
            logger.info("    Proceeding with deletion of old cluster : " + self.clusterid)
            response = client.delete_cluster(
                ClusterIdentifier=self.clusterid,
                SkipFinalClusterSnapshot=True)
            status = response['ResponseMetadata']['HTTPStatusCode']
            if status == 200:
                logger.info("    Deletion of old cluster is successful with status code " + str(status))
            else:
                logger.info("    Deletion of old cluster is unsuccessful")

        else:
            logger.info("    Zero or many old clusters exist, expected one old cluster.  No clusters deleted.  If old clusters exist, delete them manually if they are no longer needed.")

    def resize_cluster(self, nodetype, numberofnodes):
    #Resize the cluster
    #TODO: Check the space utilization on the originating cluster and resize based on that.  Need a function to check the size.
    #TODO: Check for "Available" status.  
        client = boto3.client('redshift')

        self.nodetype = nodetype
        self.numberofnodes = numberofnodes

        if self.numberofnodes > 1:
            ClusterType='multi-node'
        else:
            ClusterType='single-node'
  
        logger.info("Starting Resize to node type " + ResizeNodeType + " with " + str(ResizeNodeNumber) + " nodes.")
        response = client.modify_cluster(
            ClusterIdentifier=self.clusterid,
            ClusterType=ClusterType,
            NodeType=self.nodetype,
            NumberOfNodes=self.numberofnodes)

        #Wait for cluster restore to be fully complete
        logger.info("    Waiting for cluster resize to be completed...")
        ClusterResizeStatus = ""
        ClusterResizeStatusFinal = "available"
        while (ClusterResizeStatus!=ClusterResizeStatusFinal):
            response = client.describe_clusters(
                ClusterIdentifier=self.clusterid)
            try:
                ClusterResizeStatus = response['Clusters'][0]['ClusterStatus']
                if ClusterResizeStatus !=ClusterResizeStatusFinal:
                    logger.info("    Cluster status is: " + ClusterResizeStatus + ".  Waiting " + str(status_check_interval) + " seconds for available status")
                    time.sleep(status_check_interval)
                else:
                    logger.info("    Cluster status is: " + ClusterResizeStatus )
            except KeyError:
                traceback.print_exc()
                sys.exit(3)
        logger.info("Cluster resize has completed.")

class Route53RecordSet(str):

    def __init__(self, hostedzoneid):
        self.hostedzoneid = hostedzoneid

    def upsert_cname_record_set(self, recordset, resourcerecordsetname):
        self.recordset = recordset
        self.resourcerecordsetname = resourcerecordsetname

        client2 = boto3.client('route53')

        logger.info("Starting CNAME flip")
        response = client2.change_resource_record_sets(
            HostedZoneId=self.hostedzoneid,
            ChangeBatch= {
                        'Comment': 'add CNAME' ,
                        'Changes': [
                            {
                             'Action': 'UPSERT',
                             'ResourceRecordSet': {
                                 'Name': self.resourcerecordsetname,
                                 'Type': 'CNAME',
                                 'TTL': 300,
                                 'ResourceRecords': [{'Value': self.recordset}]
                            }
                        }]
            })
        status = response['ResponseMetadata']['HTTPStatusCode']
        if status == 200:
            logger.info("    CNAME flip is successful with status code " + str(status))
            logger.info("    Hostname applied to CNAME is: " + self.recordset)
        else:
            logger.info("    CNAME flip unsuccessful")
