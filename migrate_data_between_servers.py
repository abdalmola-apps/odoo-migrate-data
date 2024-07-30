import xmlrpc.client
import logging
from typing import List, Dict, Optional, Union

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OdooClient:
    def __init__(self, url: str, db: str, username: str, password: str):
        self.url = url
        self.db = db
        self.username = username
        self.password = password

        self.common = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common')
        self.models = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object')

        self.uid = self.authenticate()

    def authenticate(self) -> int:
        """Authenticate the user and return the user ID."""
        try:
            uid = self.common.authenticate(self.db, self.username, self.password, {})
            if not uid:
                raise Exception("Authentication failed: Please check your credentials.")
            return uid
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise

    def get_filtered_fields(self, model: str, exclude_types: Optional[List[str]] = None) -> List[str]:
        """
        Get fields from a model, excluding specified types.
        :param model: The Odoo model name as a string.
        :param exclude_types: A list of field types to exclude.
        :return: A list of field names that are not in exclude_types.
        """
        exclude_types = exclude_types or ['many2many','one2many','many2one']
        try:
            fields_info = self.models.execute_kw(self.db, self.uid, self.password, model, 'fields_get', [],
                                                 {'attributes': ['type']})
            filtered_fields = [k for k, v in fields_info.items() if v['type'] not in exclude_types]
            return filtered_fields
        except Exception as e:
            logger.error(f"Error getting filtered fields: {e}")
            raise

    def get_data_from_model(self, model: str, fields: Optional[List[str]] = None, domain: Optional[List[List[Union[str, int]]]] = None) -> List[Dict]:
        """
        Get data from any specified model using given fields and domain.
        :param model: The Odoo model name as a string.
        :param fields: A list of field names to retrieve.
        :param domain: A domain filter for the search.
        :return: A list of dictionaries containing the model data.
        """
        domain = domain or []
        fields = fields or self.get_filtered_fields(model)
        try:
            data = self.models.execute_kw(self.db, self.uid, self.password, model, 'search_read', [domain],
                                          {'fields': fields})
            return data
        except Exception as e:
            logger.error(f"Error getting data from model: {e}")
            raise

    def get_fields_from_model(self, model: str) -> List[str]:
        """
        Get fields for any model.
        :param model: The Odoo model name as a string.
        :return: A list of field names.
        """
        try:
            fields_info = self.models.execute_kw(self.db, self.uid, self.password, model, 'fields_get', [],
                                                 {'attributes': ['type']})
            return list(fields_info.keys())
        except Exception as e:
            logger.error(f"Error getting fields from model: {e}")
            raise

    def compare_field_lists(self, fields_list_1: List[str], fields_list_2: List[str]) -> Dict[str, List[str]]:
        """
        Compare two lists of fields and return the differences.
        :param fields_list_1: The first list of field names.
        :param fields_list_2: The second list of field names.
        :return: A dictionary with unique fields in each list and common fields.
        """
        fields_1_set = set(fields_list_1)
        fields_2_set = set(fields_list_2)
        return {
            'unique_to_list_1': list(fields_1_set - fields_2_set),
            'unique_to_list_2': list(fields_2_set - fields_1_set),
            'common_fields': list(fields_1_set & fields_2_set)
        }

    def create_data_on_model(self, model: str, data: List[Dict]) -> List[int]:
        """
        Create data on a specified model in Server 2.
        :param model: The Odoo model name as a string.
        :param data: A list of dictionaries containing the data to create.
        :return: A list of created record IDs.
        """
        if not data:
            return []

        created_ids = []
        for record in data:
            try:
                created_id = self.models.execute_kw(self.db, self.uid, self.password, model, 'create', [record])
                created_ids.append(created_id)
            except Exception as e:
                logger.error(f"Error creating data on model {model}: {e}")
                continue
        return created_ids

def compare_models_between_servers(server1: Dict[str, str], server2: Dict[str, str], model: str) -> Dict[str, List[str]]:
    """
    Compare fields of a model between two servers.
    :param server1: A dictionary containing connection details for the first server.
    :param server2: A dictionary containing connection details for the second server.
    :param model: The Odoo model name as a string to compare fields.
    :return: A dictionary with the comparison result.
    """
    try:
        client1 = OdooClient(server1['url'], server1['db'], server1['username'], server1['password'])
        fields1 = client1.get_filtered_fields(model)

        client2 = OdooClient(server2['url'], server2['db'], server2['username'], server2['password'])
        fields2 = client2.get_filtered_fields(model)

        return client1.compare_field_lists(fields1, fields2)
    except Exception as e:
        logger.error(f"Error comparing models between servers: {e}")
        raise


def migrate_data_between_servers(server1: Dict[str, str], server2: Dict[str, str], model: str, fields: Optional[List[str]] = None, domain: Optional[List[List[Union[str, int]]]] = None) -> List[int]:
    """
    Migrate data from Server 1 to Server 2 using common fields of the specified model.
    :param server1: A dictionary containing connection details for Server 1.
    :param server2: A dictionary containing connection details for Server 2.
    :param model: The Odoo model name as a string to migrate data.
    :param fields: Optional list of field names to read from Server 1.
    :param domain: Optional domain to filter data on Server 1.
    :return: A list of created record IDs on Server 2.
    """
    try:
        client1 = OdooClient(server1['url'], server1['db'], server1['username'], server1['password'])
        server1_data = client1.get_data_from_model(model, fields=fields, domain=domain)

        client2 = OdooClient(server2['url'], server2['db'], server2['username'], server2['password'])
        comparison_result = compare_models_between_servers(server1, server2, model)

        common_fields = comparison_result['common_fields']
        filtered_data = [{key: record[key] for key in common_fields if key in record} for record in server1_data]

        return client2.create_data_on_model(model, filtered_data)
    except Exception as e:
        logger.error(f"Error migrating data between servers: {e}")
        raise

# Example usage
if __name__ == "__main__":
    server1_details = {
        'url': "",
        'db': "",
        'username': "",
        'password': ""
    }

    server2_details = {
        'url': "",
        'db': "",
        'username': "",
        'password': ""
    }

    model_name = 'crm.tag'
    created_record_ids = migrate_data_between_servers(server1_details, server2_details, model_name)

    logger.info(f"Created Record IDs on Server 2 for model '{model_name}': {created_record_ids}")

    comparison_result = compare_models_between_servers(server1_details, server2_details, model_name)
    logger.info(f"Comparison of '{model_name}' model fields between Server 1 and Server 2:")
    logger.info(f"Unique to Server 1: {comparison_result['unique_to_list_1']}")
    logger.info(f"Unique to Server 2: {comparison_result['unique_to_list_2']}")
    logger.info(f"Common Fields: {comparison_result['common_fields']}")
