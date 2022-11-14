# The authentication is directly copied from https://github.com/cedadev/opendap-python-example/blob/master/simple_file_downloader.py
#
import os
import datetime
import ssl
from getpass import getpass


# Import third-party libraries
from cryptography import x509
from cryptography.hazmat.backends import default_backend

# from contrail.security.online_ca_client import OnlineCaClient
# pip install ContrailOnlineCAClient
from contrail.security.onlineca.client import OnlineCaClient

# Credentials defaults
DODS_FILE_CONTENTS = """HTTP.COOKIEJAR=./dods_cookies
HTTP.SSL.CERTIFICATE=./credentials.pem
HTTP.SSL.KEY=./credentials.pem
HTTP.SSL.CAPATH=./ca-trustroots
"""

DODS_FILE_PATH = os.path.expanduser('~/.dodsrc')
CERTS_DIR = os.path.expanduser('~/.certs')

if not os.path.isdir(CERTS_DIR):
    os.makedirs(CERTS_DIR)

TRUSTROOTS_DIR = os.path.join(CERTS_DIR, 'ca-trustroots')
CREDENTIALS_FILE_PATH = os.path.join(CERTS_DIR, 'credentials.pem')

TRUSTROOTS_SERVICE = 'https://slcs.ceda.ac.uk/onlineca/trustroots/'
CERT_SERVICE = 'https://slcs.ceda.ac.uk/onlineca/certificate/'


def write_dods_file_contents():

    DODS_FILE_CONTENTS = """
    HTTP.COOKIEJAR=./dods_cookies
    HTTP.SSL.CERTIFICATE={credentials_file_path}
    HTTP.SSL.KEY={credentials_file_path}
    HTTP.SSL.CAPATH={trustroots_dir}
    """.format(credentials_file_path=CREDENTIALS_FILE_PATH, trustroots_dir=TRUSTROOTS_DIR)

    with open(DODS_FILE_PATH, 'w') as dods_file:
        dods_file.write(DODS_FILE_CONTENTS)


def cert_is_valid(cert_file, min_lifetime=0):
    """
    Returns boolean - True if the certificate is in date.
    Optional argument min_lifetime is the number of seconds
    which must remain.
    :param cert_file: certificate file path.
    :param min_lifetime: minimum lifetime (seconds)
    :return: boolean
    """
    try:
        with open(cert_file, 'rb') as f:
            crt_data = f.read()
    except IOError:
        return False

    try:
        cert = x509.load_pem_x509_certificate(crt_data, default_backend())
    except ValueError:
        return False

    now = datetime.datetime.now()

    return (cert.not_valid_before <= now
            and cert.not_valid_after > now + datetime.timedelta(0, min_lifetime))
    

    
def setup_credentials(force=False):
    """
    Download and create required credentials files.
    Return True if credentials were set up.
    Return False is credentials were already set up.
    :param force: boolean
    :return: boolean
    """
    # Test for DODS_FILE and only re-get credentials if it doesn't
    # exist AND `force` is True AND certificate is in-date.
    if os.path.isfile(DODS_FILE_PATH) and not force and cert_is_valid(CREDENTIALS_FILE_PATH):
        print('[INFO] Security credentials already set up.')
        return CREDENTIALS_FILE_PATH

    onlineca_client = OnlineCaClient()
    onlineca_client.ca_cert_dir = TRUSTROOTS_DIR

    # Set up trust roots
    trustroots = onlineca_client.get_trustroots(
        TRUSTROOTS_SERVICE,
        bootstrap=True,
        write_to_ca_cert_dir=True)
    
    #username = input("CEDA username")
    #password = getpass("CEDA password")
    username = os.environ['CEDA_USERNAME']
    password = os.environ['CEDA_PASSWORD']


    # Write certificate credentials file
    key_pair, certs = onlineca_client.get_certificate(
        username,
        password,
        CERT_SERVICE,
        pem_out_filepath=CREDENTIALS_FILE_PATH)

    # Write the dodsrc credentials file
    write_dods_file_contents()

    print('[INFO] Security credentials set up.')
    return CREDENTIALS_FILE_PATH





### +++++++++  here comes the actual recipe definition +++++++++++++++++++++++
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_recipes.recipes import setup_logging


def get_ssl():
    sslcontext = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    sslcontext.load_cert_chain(setup_credentials())
    return sslcontext


def make_url(time, variable, version="4.05"):
    ## there is some peculiarities with 4.06 where the file pattern has an exception for cld
    ## the version for cld is 4.06.01 and 4.06 for all other variables...
    return f'https://dap.ceda.ac.uk/badc/cru/data/cru_ts/cru_ts_{version}/data/{variable}/cru_ts{version}.1901.1910.{variable}.dat.nc.gz'

# This is inspired by the EOBS feedstock: https://github.com/pangeo-forge/EOBS-feedstock/blob/main/feedstock/recipe.py
pattern = FilePattern(
    make_url,
    ConcatDim('time', keys=['']),
    MergeDim(name='variable', keys=["cld", "dtr", "frs", "pet", "pre", "tmn", "tmp", "tmx", "vap", "wet"]),
    fsspec_open_kwargs={'compression':'gzip', 'ssl': get_ssl()}, file_type="netcdf3"
)


recipe = XarrayZarrRecipe(pattern, target_chunks={'time': 40})