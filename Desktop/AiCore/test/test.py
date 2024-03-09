class DataCleaning:
    def init(self) -> None:
        pass

    def called_clean_store_data(self, stores_df):
        
        stores_df = stores_df.drop_duplicates(keep='first') 
        stores_df = stores_df.drop('lat', axis=1, inplace=True)
        
        stores_df = stores_df.replace('NULL', np.nan, inplace=True)
        #store_df = stores_df.dropna(how='all',  inplace=True)
        stores_df = stores_df.dropna(inplace=True)
        stores_df['opening_date'] = pd.to_datetime(stores_df['opening_date'], errors ='coerce')

        stores_df['longitude'] = stores_df['longitude'].astype('float')
        stores_df['latitude'] = stores_df['latitude'].astype('float')
        stores_df['staff_numbers'] = stores_df['staff_numbers'].astype('int')
        stores_df['store_type'] = stores_df['store_type'].astype('category')
        stores_df['country_code'] = stores_df['country_code'].astype('category')
        stores_df = stores_df.reset_index(drop=True)

        return stores_df

# def clean_card_data(self, card_df):

#     card_df = card_df.drop_duplicates(keep='first')
#     card_df = card_df[card_df['card_number'].apply(lambda x: str(x).isdigit())]
#     card_df['card_provider'] = card_df['card_provider'].apply(lambda x: re.sub(r"\d+", "", x)).str.strip('digit')
#     card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], infer_datetime_format=True, errors='coerce') 
#     card_df.dropna(inplace=True)
#     card_df = card_df.reset_index(drop=True)
#     return card_df
    


if name == 'main':

    dataCleaning = DataCleaning()
