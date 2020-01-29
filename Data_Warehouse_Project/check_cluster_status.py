import configparser
import boto3

def check_cluster_status(redshift, CLUSTER_IDENTIFIER):
    '''
    checks the cluster status at the time
    Arguments - redshift - client
                CLUSTER_IDENTIFIER - cluster identifier

    '''
    try:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier= CLUSTER_IDENTIFIER)['Clusters'][0]
        STATUS = myClusterProps['ClusterStatus']
        print('Cluster status is ', STATUS)
    except Exception as e:
        print('Cluster is not available')

def main():
    '''
    checks the cluster status
    '''
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    CLUSTER_IDENTIFIER     =   config.get("CLUSTER","CLUSTER_IDENTIFIER")
    KEY                    =   config.get('AWS','KEY')
    SECRET                 =   config.get('AWS', 'SECRET')

    redshift = boto3.client ('redshift'
                            ,region_name           = 'us-west-2'
                            ,aws_access_key_id     = KEY
                            ,aws_secret_access_key = SECRET )

    check_cluster_status(redshift
                        ,CLUSTER_IDENTIFIER)

if __name__ == "__main__":
    main()
