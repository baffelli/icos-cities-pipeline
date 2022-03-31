"""
Module to manage secrets (API keys, etc)
used to access different locations
"""
import secrets
from attr import dataclass
import yaml
from yaml import Loader
from . import files as fu
from typing import Optional, Dict, Union, List
import pathlib as pl
import itertools

@dataclass
class ServiceCredentials():
    """
    Class to represent one credential entry
    in a secrets vault
    """
    name: str
    secret: str
    user: Optional[str] = None
    
@dataclass 
class CredentialsVault():
    """
    Class to represent a vault storing
    credentials
    """
    secrets: Dict[str, List[ServiceCredentials]]
    path: Union[str, pl.Path]

    @staticmethod
    def from_file(file_path: Union[pl.Path, str]):
        with open(file_path) as input:
            config = yaml.load(input, Loader)
        all_services = [d['service'] for d in config if 'service' in d.keys()]
        secrets = {k:[ServiceCredentials(**el) for el in g] for k,g in itertools.groupby(all_services, lambda x: x['name'])}
        return CredentialsVault(secrets=secrets, path=file_path)
                
    def get_secret(self, service: str, user:str=None) -> str:
        try:
            all_sec = self.secrets[service]
        except KeyError:
            raise KeyError(f"Service with name {service} does not exists in this vault: {self.path}")
        if user:
            try:
                res = [s for s in all_sec if s.user==user][0]
            except:
                raise KeyError(f'User {user} for {service} does not exists in this vault: {self.path}')
        else:
            res = all_sec[0]
        return res.secret
        

def get_key(service: str, user:str=None, file:str=f'/newhome/{fu.get_user()}/secrets.yml') -> str:
    vault = CredentialsVault.from_file(file)
    return vault.get_secret(service, user)
