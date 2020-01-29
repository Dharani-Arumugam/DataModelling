import pandas as pd
import boto3
import json
import configparser
import psycopg2

def open_ports(ec2, myClusterProps, PORT):
    '''
    Update clusters security group to allow access through redshift port
    Arguments :  ec2 instance
                ,myClusterProps - Cluster properties
                ,PORT           - port number
    '''

    try:
        vpc         = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg   = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName     =   defaultSg.group_name,
            CidrIp        =   '0.0.0.0/0',
            IpProtocol    =   'TCP',
            FromPort      =   int(PORT),
            ToPort        =   int(PORT)
        )
    except Exception as e:
        print(e)

def get_cluster_props(redshift, CLUSTER_IDENTIFIER):
    '''
    Retrieve Redshift clusters properties
    Arguments  :  redshift instance
                 ,CLUSTER_IDENTIFIER
    Returns    : myClusterProps - cluster properties
    '''

    def prettyRedshiftProps(props):
        '''
        Retrieves the cluster properties from describe_clusters
        Arguments : properties variable
        '''
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier"
                      ,"NodeType"
                      ,"ClusterStatus"
                      ,"MasterUsername"
                      ,"DBName"
                      ,"Endpoint"
                      ,"NumberOfNodes"
                      ,"VpcId"]
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    myClusterProps = redshift.describe_clusters(ClusterIdentifier= CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)

    ENDPOINT = myClusterProps['Endpoint']['Address']
    ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("ENDPOINT :: ", ENDPOINT)
    print("ROLE_ARN :: ", ROLE_ARN)
    return myClusterProps

def create_cluster ( redshift
                    ,roleArn
                    ,CLUSTER_TYPE
                    ,NODE_TYPE
                    ,NUM_NODES
                    ,DB_NAME
                    ,CLUSTER_IDENTIFIER
                    ,DB_USER
                    ,DB_PASSWORD):
    '''
    Create a new cluster in the AWS redshift
    Arguments :  redshift           - AWS redshift client
                 roleArn            - IAM role Arn values
                 CLUSTER_TYPE       - Type of cluster
                 NODE_TYPE          - Node type of cluster
                 NUM_NODES          - Number of nodes for cluster
                 DB_NAME            - Cluster Database name
                 CLUSTER_IDENTIFIER - cluster Identifier
                 DB_USER            - Cluster Database name
                 DB_PASSWORD        - CLuster Database password
    '''
    try:
        response = redshift.create_cluster(
            #HW
            ClusterType     =  CLUSTER_TYPE,
            NodeType        =  NODE_TYPE,
            NumberOfNodes   =  int(NUM_NODES),

            #Identifiers & Credentials
            DBName            =  DB_NAME,
            ClusterIdentifier =  CLUSTER_IDENTIFIER,
            MasterUsername    =  DB_USER,
            MasterUserPassword=  DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]
            )
    except Exception as e:
        print(e)

def create_iam_role(iam ,IAM_ROLE_NAME):
    '''
    Creates an IAM role
    Arguments : iam - IAM client
                IAM_ROLE_NAME - Role name
    Returns   : roleArn - ARN of an IAM Role
    '''
    try:
        print("Creating a new IAM Role")
        dwhRole = iam.create_role(  Path        ='/'
                                   ,RoleName    = IAM_ROLE_NAME
                                   ,Description = "Allows Redshift clusters to call AWS services on your behalf."
                                   ,AssumeRolePolicyDocument = json.dumps(
                                        {'Statement': [{'Action': 'sts:AssumeRole',
                                         'Effect'   : 'Allow',
                                         'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                         'Version'  : '2012-10-17'})
                                )
    except Exception as e:
        print(e)

    print("Attaching Policy")
    iam.attach_role_policy( RoleName   = IAM_ROLE_NAME
                           ,PolicyArn  = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']

    print("Get the IAM Role ARN")
    roleArn = iam.get_role(RoleName = IAM_ROLE_NAME)['Role']['Arn']

    return roleArn


def main():
    '''
    Fetches the configuration values from dwh.cfg read_file
    and connects to database
    '''
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    KEY                    =   config.get('AWS','KEY')
    SECRET                 =   config.get('AWS', 'SECRET')

    CLUSTER_TYPE           =   config.get('CLUSTER','CLUSTER_TYPE')
    NUM_NODES              =   config.get('CLUSTER','NUM_NODES')
    NODE_TYPE              =   config.get('CLUSTER','NODE_TYPE')

    CLUSTER_IDENTIFIER     =   config.get("CLUSTER","CLUSTER_IDENTIFIER")
    DB_NAME                =   config.get("CLUSTER","DB_NAME")
    DB_USER                =   config.get("CLUSTER","DB_USER")
    DB_PASSWORD            =   config.get("CLUSTER","DB_PASSWORD")
    PORT                   =   config.get("CLUSTER","PORT")

    IAM_ROLE_NAME          =   config.get("CLUSTER", "IAM_ROLE_NAME")


    ec2   = boto3.resource ('ec2'
                            ,region_name           = 'us-west-2'
                            ,aws_access_key_id     = KEY
                            ,aws_secret_access_key = SECRET )
    s3    = boto3.resource ('s3'
                            ,region_name           = 'us-west-2'
                            ,aws_access_key_id     = KEY
                            ,aws_secret_access_key = SECRET )
    iam      = boto3.client ('iam'
                             ,region_name           = 'us-west-2'
                             ,aws_access_key_id     = KEY
                             ,aws_secret_access_key = SECRET )
    redshift = boto3.client ('redshift'
                             ,region_name           = 'us-west-2'
                             ,aws_access_key_id     = KEY
                             ,aws_secret_access_key = SECRET )

    roleArn = create_iam_role(  iam
                               ,IAM_ROLE_NAME)

    create_cluster ( redshift
                     ,roleArn
                     ,CLUSTER_TYPE
                     ,NODE_TYPE
                     ,NUM_NODES
                     ,DB_NAME
                     ,CLUSTER_IDENTIFIER
                     ,DB_USER
                     ,DB_PASSWORD)

    myClusterProps = get_cluster_props( redshift
                                       ,CLUSTER_IDENTIFIER)

    open_ports(ec2, myClusterProps, PORT)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Connected')

    conn.close()


if __name__ == "__main__":
    main()
