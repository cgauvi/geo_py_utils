
import pandas as pd
import geopandas as gpd
from os.path import join, exists

from ben_py_utils.misc.cache import Cache_wrapper
 
from geo_py_utils.misc.constants import DATA_DIR

QC_CITY_NEIGH_URL = "https://www.donneesquebec.ca/recherche/dataset/5b1ae6f2-6719-46df-bd2f-e57a7034c917/resource/436c85aa-88d9-4e57-9095-b72b776a71a0/download/vdq-quartier.geojson"


@Cache_wrapper(path_cache=join(DATA_DIR, "cache", "qc_neighborhoods.parquet"))
def download_qc_city_neighborhoods():
    """ Download the neighborhood polygons for Qc City (city proper only - corresponds to census sub division) 
    """
    url_qc_city = QC_CITY_NEIGH_URL
    shp_qc_city = gpd.read_file(url_qc_city).to_crs(4326)        

    return shp_qc_city



def get_qc_city_bbox()-> dict : 

    """ Build a spatial clause: take only policies within Quebec City bounding box. 

    Returns:
        bounding box: dict representing qc city bounding box
    """

    # Get the qc city neighborhoods
    shp_qc_city = download_qc_city_neighborhoods()

    # Take only policies within Quebec City bounding box
    min_lng, min_lat, max_lng, max_lat = shp_qc_city.total_bounds


    return {"min_lng": min_lng, 
            "min_lat": min_lat, 
            "max_lng": max_lng, 
            "max_lat": max_lat}



class DownloadQcAdmBoundaries:

    QC_PROV_ADM_BOUND_URL = "https://diffusion.mern.gouv.qc.ca/Diffusion/RGQ/Vectoriel/Theme/Local/SDA_20k/SHP/SHP.zip"

    QC_PROV_ADM_BOUND_MRC = "mrc_s"
    QC_PROV_ADM_BOUND_MUNI = "munic_s"
    QC_PROV_ADM_BOUND_METRO = "comet_s'"

 
    def __init__(self,
                geo_level=QC_PROV_ADM_BOUND_MRC,
                data_download_path=join(DATA_DIR, 'qc_adm_regions')
                ) :
        
        self.geo_level = geo_level
        self.data_download_path = data_download_path


    def _download_qc_administrative_boundaries_wrappe(self):

        """Download the zip file to disk.
        """

        # Unzip the entire directory of shp files
        try:
            download_zip_shp(
                url = DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_URL,
                data_download_path = self.data_download_path
            )
        except Exception as e :
            pass
    

 
    def get_qc_administrative_boundaries(self) -> gpd.GeoDataFrame :
        """ Download the neighborhood polygons for Qc City (city proper only - corresponds to census sub division) 
        """

        # point to data path of unzipped folder
        path_shp = join(DATA_DIR, 'qc_adm_regions', 'SHP','Sda', 'version_courante','SHP', f'{self.geo_level}].shp')

        # Unzip if required
        if not exists(path_shp):
            self._download_qc_administrative_boundaries_wrappe()

        # Read in the desired polygons
        shp_boundary = gpd.read_file()

        return shp_boundary
        

  

class DownloadQcDissolvedMrc(DownloadQcAdmBoundaries):

    def __init__(self,
                data_download_path=join(DATA_DIR, 'qc_adm_regions')
                ) :

        super.__init__(DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MRC , data_download_path)
                    

    def _create_dict_mapping(self) -> dict:
            """Create the hardcoded dict to map values from the official boundary files to the opus data
            """

            DICT_MAPPING={
                "Abitibi-Témiscamingue": None,
                    "Bas-Saint-Laurent": None,
                    "Capitale-Nationale":  "QUÉBEC",
                    "Centre-du-Québec":  "CENTRE-DU-QUÉBEC",
                    "Chaudière-Appalaches":  "CHAUDIÈRE-APPALACHES",
                    "Côte-Nord": "CÔTE-NORD",
                    "Estrie": "ESTRIE",
                    "Gaspésie - Îles-de-la-Madeleine": "GASPÉSIE/ÎLES-DE-LA-MADELAINE",
                    "Lanaudière" :  "LANAUDIÈRE",
                    "Laurentides": "LAURENTIDES",
                    "Laval":"LAVAL",
                    "Mauricie":  "MAURICIE/BOIS-FRANCS",
                    "Montréal": "MONTRÉAL",
                    "Montérégie":  "MONTÉRÉGIE",
                    "Nord-du-Québec": None,
                    "Outaouais": "OUTAOUAIS",
                    "Saguenay - Lac-Saint-Jean":  "SAGUENAY LAC ST-JEAN"
                }

            df_mapping = pd.DataFrame.\
                from_dict(DICT_MAPPING, orient='index').\
                reset_index()

            df_mapping.columns = ['MRS_NM_REG', 'DOMAINE_OPUS']

            return df_mapping


        def get_qc_administrative_boundaries(self) -> gpd.GeoDataFrame:
            """Take the MRC polygons and dissolve them into the large regions we want (based on `MRS_NM_REG`)

            Dissolving is useful since there are arguably too many details in the dataset
            For instance, there are many sub polygons associated with 'Cote-Nord', many of whom have the "Nouveau toponyme à venir" label (MRS_NM_MRC column) 
                -> it is not extremelly useful to keep all these precise polygons

            Args:
                self (_type_): _description_
            Returns:
                gpd.GeoDataFrame: dissolve geodf
            """

            # point to data path of unzipped folder
            path_shp = join(DATA_DIR, 'qc_adm_regions', 'SHP','Sda', 'version_courante','SHP', 
                            f'{self.geo_level}.shp')

            # Unzip if required
            if not exists(path_shp):
                self._download_qc_administrative_boundaries_wrappe()

            # Dissolve according to the large region names: MRS_NM_REG 
            shp_mrc_dissolved = ss[['MRS_NM_REG','geometry']].\
                        dissolve(by='MRS_NM_REG').\
                        reset_index()

            # Map the opus values
            dict_mapping = self._create_dict_mapping()
            shp_mrc_dissolved.merge(df_mapping, on='MRS_NM_REG')

            return shp_mrc_dissolved


if __name__ == '__main__':

     qc_boundaries_extracter = DownloadQcAdmBoundaries()

     shp_mrc = qc_boundaries_extracter.get_dissolved_mrc()