import configparser
import boto3

def delete_cluster( redshift,CLUSTER_IDENTIFIER, iam,IAM_ROLE_NAME ):
    '''
    Deletes the cluster and role after detaching the role policy
    Arguments  : redshift           - client
                 CLUSTER_IDENTIFIER - cluster indentifier
                 iam                - IAM client
                 IAM_ROLE_NAME      - Role name
    '''
    try:
        redshift.delete_cluster( ClusterIdentifier          = CLUSTER_IDENTIFIER
                                ,SkipFinalClusterSnapshot   = True)
        print('cluster deleted')
    except Exception as e:
        print('Cluster could not be deleted',e)

    try:
        iam.detach_role_policy( RoleName   = IAM_ROLE_NAME
                               ,PolicyArn  = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        print('role policy detached')
    except Exception as e:
        print('Role policy not detached')

    try:
        iam.delete_role(RoleName =IAM_ROLE_NAME)
        print('Role deleted')
    except Exception as e:
        print('Role not deleted')

def main():
    '''
    Fetches the configuration values from dwh.cfg read_file
    and calls delete_cluster
    '''
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    CLUSTER_IDENTIFIER     =   config.get("CLUSTER","CLUSTER_IDENTIFIER")
    KEY                    =   config.get('AWS','KEY')
    SECRET                 =   config.get('AWS', 'SECRET')

    IAM_ROLE_NAME          =   config.get("CLUSTER", "IAM_ROLE_NAME")


    redshift = boto3.client ('redshift'
                            ,region_name           = 'us-west-2'
                            ,aws_access_key_id     = KEY
                            ,aws_secret_access_key = SECRET )
    iam      = boto3.client ('iam'
                            ,region_name           = 'us-west-2'
                            ,aws_access_key_id     = KEY
                            ,aws_secret_access_key = SECRET )


    delete_cluster( redshift
                   ,CLUSTER_IDENTIFIER
                   ,iam
                   ,IAM_ROLE_NAME)

if __name__ == "__main__":
    main()
