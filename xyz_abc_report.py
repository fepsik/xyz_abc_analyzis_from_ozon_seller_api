import pandas as pd
import requests


class CreateXYZABCReport:
    def __init__(self, api_key, client_id, date_from, date_to):
        self.metrics = ['hits_view_pdp', 'delivered_units', 'revenue']
        self.dimensions = ['month', 'sku']
        self.api_key = api_key
        self.client_id = client_id
        self.date_from = date_from
        self.date_to = date_to
        self.auth_headers = {"Client-Id": self.client_id, "Api-Key": self.api_key}
        self.url = 'https://api-seller.ozon.ru'

    def get_data_from_ozon(self):
        method = '/v1/analytics/data'
        query_body = {
            "date_from": self.date_from,
            "date_to": self.date_to,
            "dimension": self.dimensions,
            "filters": [
                {
                    "key": "hits_view_pdp",
                    "op": "GT",
                    "value": "0"
                }
            ],
            "limit": 1000,
            "metrics": self.metrics,
            "offset": 0,
            "sort": [
                {
                    "key": self.dimensions[0],
                    "order": "ASC"
                }
            ]
        }
        response = requests.post(url=self.url + method, headers=self.auth_headers, json=query_body)
        len_resp = len(response.json().get('result').get('data'))
        data = response.json().get('result').get('data')
        n = 1
        while len_resp == 1000:
            query_body['offset'] = n * 1000
            response = requests.post(url=self.url + method, headers=self.auth_headers, json=query_body)
            data += response.json().get('result').get('data')
            len_resp = len(response.json().get('result').get('data'))
            n += 1
        finale_dict = {}
        for n, dimension in enumerate(self.dimensions):
            finale_dict[f'{dimension}_id'] = [x.get('dimensions')[n].get('id') for x in data]
            finale_dict[f'{dimension}_name'] = [x.get('dimensions')[n].get('name') for x in data]
        for i, metric in enumerate(self.metrics):
            finale_dict[metric] = [x.get('metrics')[i] for x in data]
        # finale_dict
        offers_data = pd.DataFrame(finale_dict)
        return offers_data

    def calculate_abc_xyz(self, offers_data):
        df_12m_units = offers_data.groupby(['sku_id', 'month_id'])[
            'delivered_units'].sum().to_frame().reset_index().pivot(index='sku_id', columns='month_id',
                                                                    values='delivered_units') \
            .reset_index().fillna(0)
        df_12m_units['std_demand'] = df_12m_units[list(df_12m_units.columns[1:])].std(axis=1)
        df_12m_units['total_demand'] = df_12m_units.iloc[:, 1:-1].sum(axis=1)
        df_12m_units['avg_demand'] = df_12m_units.iloc[:, 1:-2].mean(axis=1)
        df_12m_units['cov_demand'] = df_12m_units['std_demand'] / df_12m_units['avg_demand']

        df_12m_units['xyz_class'] = df_12m_units['cov_demand'].apply(self.xyz_classify_product)

        df_skus = offers_data.groupby('sku_id').agg(
            total_units=('delivered_units', 'sum'),
            total_revenue=('revenue', 'sum'),
        ).sort_values(by='total_revenue', ascending=False).reset_index()

        df_skus['revenue_cumsum'] = df_skus['total_revenue'].cumsum()
        df_skus['revenue_total'] = df_skus['total_revenue'].sum().round(2)
        df_skus['revenue_running_percentage'] = (df_skus['revenue_cumsum'] / df_skus['revenue_total']) * 100

        df_skus['abc_class'] = df_skus['revenue_running_percentage'].apply(self.abc_classify_product)
        df_skus['abc_rank'] = df_skus['revenue_running_percentage'].rank().astype(int)

        df_abc = df_skus.groupby('abc_class').agg(
            total_skus=('sku_id', 'nunique'),
            total_units=('total_units', sum),
            total_revenue=('total_revenue', sum),
        ).reset_index()

        df_abc.total_revenue = df_abc.total_revenue.round(2)

        df_abc = df_skus[['sku_id', 'abc_class', 'abc_rank', 'total_revenue']]
        df_xyz = df_12m_units.copy()
        df_abc_xyz = df_abc.merge(df_xyz, on='sku_id', how='left')

        df_abc_xyz['abc_xyz_class'] = df_abc_xyz['abc_class'].astype(str) + df_abc_xyz['xyz_class'].astype(str)

        prod_data = offers_data.groupby(['sku_id', 'sku_name']).agg(
            {'hits_view_pdp': 'sum'}).reset_index()

        final_data = df_abc_xyz.sort_values('total_revenue', ascending=False).merge(prod_data, on='sku_id', how='left')
        return final_data

    @staticmethod
    def xyz_classify_product(cov):
        """Apply an XYZ classification to each product based on
        its coefficient of variation in order quantity.
        :param cov: Coefficient of variation in order quantity for SKU
        :return: XYZ inventory classification class
        """
        if cov <= 1:
            return 'X'
        elif 1 < cov <= 1.5:
            return 'Y'
        else:
            return 'Z'

    @staticmethod
    def abc_classify_product(percentage):
        """Apply an ABC classification to each product based on
        its ranked percentage revenue contribution. Any split
        can be used to suit your data.

        :param percentage: Running percentage of revenue contributed
        :return: ABC inventory classification
        """

        if 0 < percentage <= 80:
            return 'A'
        elif 80 < percentage <= 90:
            return 'B'
        else:
            return 'C'

    def do_everything_and_get_df(self):
        data = self.get_data_from_ozon()
        report = self.calculate_abc_xyz(data)
        return report
