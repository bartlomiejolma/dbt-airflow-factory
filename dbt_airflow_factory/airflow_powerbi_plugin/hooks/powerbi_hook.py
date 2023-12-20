import requests
import json
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

class PowerBIHook(BaseHook):
    """
    Hook to interact with the Power BI REST API.

    :param client_id: Power BI App ID used to identify the application
        registered to have access to the REST API.
    :param conn_id: Airflow Connection ID that contains the connection
        information for the Power BI account used for authentication.
    """


    def __init__(self,
                 powerbi_conn_id: str):
        super().__init__()
        self.powerbi_conn_id = powerbi_conn_id
        self.header = None
        self.base_url = 'https://api.powerbi.com/v1.0/myorg'

    def get_refresh_history(self,
                            dataset_key: str,
                            group_id: str = None,
                            top: int = None) -> dict:
        """
        Returns the refresh history of the specified dataset from
        "My Workspace" when no `group id` is specified or from the specified
        workspace when `group id` is specified.

        https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/getrefreshhistory
        https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/getrefreshhistoryingroup

        :param dataset_key: The dataset id.
        :param group_id: The workspace id.
        :param top: The requested number of entries in the refresh history.
            If not provided, the default is all available entries.
        :return: dict object.
        """
        url = self.base_url
        # add the group id if it is specified
        if group_id:
            url += f'/groups/{group_id}'

        # add the dataset key
        url += f'/datasets/{dataset_key}/refreshes'

        # add the `top` parameter if it is specified
        if top:
            url += f'?$top={top}'

        r = self._send_request('GET', url=url)
        return r.json()

    def refresh_dataset(self, dataset_key: str, group_id: str = None) -> None:
        """
        Triggers a refresh for the specified dataset from "My Workspace" if
        no `group id` is specified or from the specified workspace when
        `group id` is specified.

        https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/refreshdataset
        https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/refreshdatasetingroup

        :param dataset_key: The dataset id.
        :param group_id: The workspace id.
        """
        url = self.base_url

        # add the group id if it is specified
        if group_id:
            url += f'/groups/{group_id}'

        # add the dataset key
        url += f'/datasets/{dataset_key}/refreshes'

        self._send_request('POST', url=url)

    def _get_token(self) -> str:
        """
        Retrieve the `access token` used to authenticate against the API.
        """
        pbi_connection = self.get_connection(self.powerbi_conn_id)
        connection_details = json.loads(pbi_connection.get_extra())
        if not connection_details["client_secret"]:
            raise AirflowException('No `client_secret` specified in connection.')

        if not connection_details["client_id"]:
            raise AirflowException('No `client_id` specified in connection.')

        if not connection_details["tenant_id"]:
            raise AirflowException('No `tenant_id` specified in connection.')

        auth_url = f"https://login.microsoftonline.com/{connection_details['tenant_id']}/oauth2/token"
        resource_url = 'https://analysis.windows.net/powerbi/api'

        data = {
            'grant_type': 'client_credentials',
            'scope': f"{resource_url}/.default",
            'resource': resource_url,
            'client_id': connection_details["client_id"],
            'client_secret': connection_details["client_secret"],
        }
        r = requests.post(auth_url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
        r.raise_for_status()
        return r.json().get('access_token')

    def _send_request(self,
                      request_type: str,
                      url: str,
                      **kwargs) -> requests.Response:
        """
        Send a request to the Power BI REST API.

        This method checks to see if authorisation token has been retrieved and
        the request `header` has been built using it. If not then it will
        establish the connection to perform this action on the first call. It
        is important to NOT have this connection established as part of the
        initialisation of the hook to prevent a Power BI API call each time
        the Airflow scheduler refreshes the DAGS.


        :param request_type: Request type (GET, POST, PUT etc.).
        :param url: The URL against which the request needs to be made.
        :return: requests.Response
        """
        if not self.header:
            self.header = {'Authorization': f'Bearer {self._get_token()}'}

        request_funcs = {
            'GET': requests.get,
            'POST': requests.post
        }

        func = request_funcs.get(request_type.upper())

        if not func:
            raise AirflowException(
                f'Request type of {request_type.upper()} not supported.'
            )

        r = func(url=url, headers=self.header, **kwargs)
        r.raise_for_status()
        return r
