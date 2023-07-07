
import pandas as pd
import geopandas as gpd
from os.path import join, exists
import logging 

from geo_py_utils.etl.download_zip import download_zip_shp
from geo_py_utils.misc.cache import Cache_wrapper
from geo_py_utils.misc.constants import DATA_DIR


DEFAULT_QC_CITY_NEIGH_URL = "https://www.donneesquebec.ca/recherche/dataset/5b1ae6f2-6719-46df-bd2f-e57a7034c917/resource/436c85aa-88d9-4e57-9095-b72b776a71a0/download/vdq-quartier.geojson"

logger = logging.getLogger(__file__)

@Cache_wrapper(path_cache=join(DATA_DIR, "cache", "qc_neighborhoods.parquet"))
def download_qc_city_neighborhoods(url_qc_city = DEFAULT_QC_CITY_NEIGH_URL) -> gpd.GeoDataFrame:

    """ Download the neighborhood polygons for Qc City (city proper only - corresponds to census sub division) 
    Args: 
        url_qc_city (str, optional): url to qc open data
    Returns:
       shp_qc:  gpd.GeoDataFrame 
    """
    
    shp_qc_city = gpd.read_file(url_qc_city).to_crs(4326)        

    return shp_qc_city



def get_qc_city_bbox(**qc_city_kwargs)-> dict : 

    """ Build a spatial clause: take only policies within Quebec City bounding box. 
    Args:
        qc_city_kwargs
    Returns:
        bounding box: dict representing qc city bounding box
    """

    # Get the qc city neighborhoods
    shp_qc_city = download_qc_city_neighborhoods(**qc_city_kwargs)

    # Take only policies within Quebec City bounding box
    min_lng, min_lat, max_lng, max_lat = shp_qc_city.total_bounds

    return {"min_lng": min_lng, 
            "min_lat": min_lat, 
            "max_lng": max_lng, 
            "max_lat": max_lat}



class DownloadQcAdmBoundaries:
    """Get raw Qc administrative polyons.

    Select one of the following by using constants 

    - ADM_REG : "mrc_s":  DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MRC 
    - Municipalities: "munic_s": DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MUNI
    - Communauté urbaine: "comet_s": DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_METRO 
    - Arrondissements: "arron_s": DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_ARROND

    Attributes:
        geo_level (_type_, optional): _description_. Defaults to QC_PROV_ADM_BOUND_MRC.
        data_download_path (_type_, optional): _description_. Defaults to join(DATA_DIR, 'qc_adm_regions').
    """
    QC_PROV_ADM_BOUND_URL = "https://diffusion.mern.gouv.qc.ca/Diffusion/RGQ/Vectoriel/Theme/Local/SDA_20k/SHP/SHP.zip"

    QC_PROV_ADM_BOUND_MRC = "mrc_s"
    QC_PROV_ADM_BOUND_MUNI = "munic_s"
    QC_PROV_ADM_BOUND_METRO = "comet_s"
    QC_PROV_ADM_BOUND_ARROND = "arron_s"

    def __init__(self,
                geo_level=QC_PROV_ADM_BOUND_MRC,
                data_download_path=join(DATA_DIR, 'qc_adm_regions')
                ) :

        
        self.geo_level = geo_level
        self.data_download_path = data_download_path
 

    def _download_qc_administrative_boundaries(self):

        """Download the administrative qc polygons & unzip to disk
        """

        # Unzip the entire directory of shp files - but dont try to read it in 
        download_zip_shp(
                url=DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_URL,
                data_download_path=self.data_download_path,
                return_shp_file=False
            )

 
    def get_qc_administrative_boundaries(self) -> gpd.GeoDataFrame :
        """ Get the administrative qc polygons 
        """

        # point to data path of unzipped folder
        path_shp = join(DATA_DIR, 'qc_adm_regions', 'SHP','Sda', 'version_courante','SHP', f'{self.geo_level}.shp')

        # Unzip if required
        if not exists(path_shp):
            self._download_qc_administrative_boundaries()

        assert exists(path_shp)

        # Read in the desired polygons
        shp_boundary = gpd.read_file(path_shp)

        return shp_boundary

 
        
class DownloadQcBoroughs(DownloadQcAdmBoundaries):
        
    """Get the raw boroughs (arrondissements) 
    
    Boroughs only present in:

    - Mtl
    - Quebec
    - Saguenay
    - A few other places

    Inherits from DownloadQcAdmBoundaries

    No manipuliation/dissolving

    Attributes:
        data_download_path (_type_, optional): _description_. Defaults to join(DATA_DIR, 'qc_adm_regions').
        filter_out_unknown_muni (bool, optional): Remove 'Toponyme à venir' . Defaults to True
    """

 
    def __init__(self,
                data_download_path=join(DATA_DIR, 'qc_boroughs')
                ) :


        super().__init__(DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_ARROND, data_download_path)


    def get_raw_data(self) -> gpd.GeoDataFrame:
        """Convenience method/synctacic sugar for subclasses.

        child.get_raw_data() same as child.__super__().get_qc_administrative_boundaries()

        Returns:
            gpd.GeoDataFrame: _description_
        """
        return super().get_qc_administrative_boundaries()

  
    def get_qc_administrative_boundaries(self) -> gpd.GeoDataFrame:
        
        return self.get_raw_data()



class DownloadQcDissolvedMunicipalities(DownloadQcAdmBoundaries):
        
    """Get the raw regions representing the 'usual' Quebec municipalities boundaries

    *Dont* dissolve by muni name: makes no sense. Ste-Sabine in Les Etchemins not the same as ste-sabine in Brome-Missisquoi
    *Do* dissolve by muni name + muni code

    Inherits from DownloadQcAdmBoundaries

    Attributes:
        data_download_path (_type_, optional): _description_. Defaults to join(DATA_DIR, 'qc_adm_regions').
        filter_out_unknown_muni (bool, optional): Remove 'Toponyme à venir' . Defaults to True
    """

    MUNI_DISSOLVE_ID_COL = "MUS_NM_MUN"  # 'typical' names we are used to seeing
    MUNI_DISSOLVE_CODE_COL = "MUS_CO_GEO" # almost! a unique ID: some contiguous polygons have duplicate ids: fixed by dissolving
 
 
    def __init__(self,
                data_download_path=join(DATA_DIR, 'qc_adm_regions'),
                filter_out_unknown_muni=True
                ) :


        super().__init__(DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MUNI, data_download_path)

        self.filter_out_unknown_muni = filter_out_unknown_muni
        self.path_cache = join(DATA_DIR, "cache", f"qc_adm_regions_MUNI_DISSOLVED_{filter_out_unknown_muni}.parquet")

    def get_raw_data(self) -> gpd.GeoDataFrame:
        """Convenience method/synctacic sugar for subclasses.

        child.get_raw_data() same as child.__super__().get_qc_administrative_boundaries()

        Returns:
            gpd.GeoDataFrame: _description_
        """
        return super().get_qc_administrative_boundaries()

    def get_qc_administrative_boundaries(self) -> gpd.GeoDataFrame: 
        fun = Cache_wrapper(path_cache=self.path_cache)(self.get_qc_administrative_boundaries_wrappee)
        return fun()
  
    def get_qc_administrative_boundaries_wrappee(self) -> gpd.GeoDataFrame:
        
        # Call the parent method to download the raw polygons
        shp_mrc_raw = self.get_raw_data()

        # Filter out uselesss garbagge
        if self.filter_out_unknown_muni:
            logger.warning('Filtering out uninhabited municipalities with `Toponyme à venir`')
            shp_mrc_raw = shp_mrc_raw[shp_mrc_raw["MUS_NM_MUN"] != 'Toponyme à venir']

        # Dissolve according to the large region names:  
        cols_unique_id = [DownloadQcDissolvedMunicipalities.MUNI_DISSOLVE_ID_COL, DownloadQcDissolvedMunicipalities.MUNI_DISSOLVE_CODE_COL]
        cols_unique_with_geo = [*cols_unique_id, 'geometry']

        shp_muni_dissolved = shp_mrc_raw[cols_unique_with_geo].\
            dissolve(by=DownloadQcDissolvedMunicipalities.MUNI_DISSOLVE_CODE_COL).\
            reset_index()

        return shp_muni_dissolved




class DownloadQcDissolvedAdmReg(DownloadQcAdmBoundaries):
        
    """Get the dissolved 17 regions representing the 'usual' Quebec administrative boundaries.

    Convoluted: takes in the MRC data and then dissolves by MRS_CO_REG (adm region code)
    Would also be possible to read in `Sda/version_courante/SHP/regio_s.shp` layer from DownloadQcAdmBoundaries and dissolve by  MRS_CO_REG

    Dissolving not super important, but avoids having 1 extra polygon for Cote-Nord near Labrador
     -> So we have 17 unique polygons rather than 18
     -> Dissolving applicable whether we start from regio_s or mrc_s layers 

    Inherits from DownloadQcAdmBoundaries

    Attributes:
        data_download_path (_type_, optional): _description_. Defaults to join(DATA_DIR, 'qc_adm_regions').
    """

    ADM_REG_DISSOLVE_ID_COL = "MRS_NM_REG"  # 'typical' names we are used to seeing: correspond to the keys in DICT_MAPPING
    ADM_REG_DISSOLVE_CODE_COL = "MRS_CO_REG"

    PATH_CACHE = join(DATA_DIR, "cache", "qc_adm_regions_ADM_REG_DISSOLVED_17.parquet")

    DICT_MAPPING={
                "Abitibi-Témiscamingue":    None,
                "Bas-Saint-Laurent": None,
                "Capitale-Nationale":  "QUÉBEC" ,
                "Centre-du-Québec":  "CENTRE-DU-QUÉBEC", 
                "Chaudière-Appalaches":  "CHAUDIÈRE-APPALACHES", 
                "Côte-Nord": "CÔTE-NORD", 
                "Estrie": "ESTRIE", 
                "Gaspésie--Îles-de-la-Madeleine": "GASPÉSIE/ÎLES-DE-LA-MADELAINE", # watch out with the names ... some characters are different from raw data when reading in with fiona
                "Lanaudière" :  "LANAUDIÈRE",
                "Laurentides": "LAURENTIDES"  ,
                "Laval":"LAVAL" ,
                "Mauricie":  "MAURICIE/BOIS-FRANCS" ,
                "Montréal": "MONTRÉAL" ,  
                "Montérégie":  "MONTÉRÉGIE" , 
                "Nord-du-Québec": None,
                "Outaouais": "OUTAOUAIS" ,
                "Saguenay--Lac-Saint-Jean" :  "SAGUENAY LAC ST-JEAN" # also watch out for -- here - like Iles de la Madelaine above
        }

    def __init__(self,
                data_download_path=join(DATA_DIR, 'qc_adm_regions')
                ) :


        super().__init__(DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MRC , data_download_path)
        self.raw_data = None

    def _create_dict_mapping(self) -> dict:
            """Create the hardcoded dict to map values from the official boundary files to the opus data
            """

            df_mapping = pd.DataFrame.\
                from_dict(DownloadQcDissolvedAdmReg.DICT_MAPPING, orient='index').\
                reset_index()

            df_mapping.columns = ['MRS_NM_REG', 'DOMAINE_OPUS']

            return df_mapping

    @Cache_wrapper(path_cache=PATH_CACHE)
    def get_qc_administrative_boundaries(self) -> gpd.GeoDataFrame:
        """Take the ADM_REG polygons and dissolve them into the large regions we want (based on `MRS_NM_REG`)

        Dissolving is useful since there are arguably too many details in the dataset
        For instance, there are many sub polygons associated with 'Cote-Nord', many of whom have the "Nouveau toponyme à venir" label (MRS_NM_ADM_REG column) 
                -> it is not extremelly useful to keep all these precise polygons

        Args:
            self (_type_): _description_
        Returns:
            gpd.GeoDataFrame: dissolve geodf
        """

        # Call the parent method to download the raw polygons
        shp_mrc_raw = super().get_qc_administrative_boundaries()

        # Dissolve according to the large region names: MRS_NM_REG + MRS_CO_REG
        cols_unique_id = [DownloadQcDissolvedAdmReg.ADM_REG_DISSOLVE_ID_COL, DownloadQcDissolvedAdmReg.ADM_REG_DISSOLVE_CODE_COL]
        cols_unique_with_geo = [*cols_unique_id, 'geometry']

        shp_mrc_dissolved = shp_mrc_raw[cols_unique_with_geo].\
            dissolve(by=cols_unique_id).\
            reset_index()

        # Map the opus values
        df_mapping = self._create_dict_mapping()
        shp_mrc_dissolved_merged = shp_mrc_dissolved.merge(df_mapping, on=DownloadQcDissolvedAdmReg.ADM_REG_DISSOLVE_ID_COL)

        return shp_mrc_dissolved_merged


    def get_raw_data(self) -> gpd.GeoDataFrame:
        """Convenience method/synctacic sugar for subclasses.

        child.get_raw_data() same as child.__super__().get_qc_administrative_boundaries()

        Returns:
            gpd.GeoDataFrame: _description_
        """
        return super().get_qc_administrative_boundaries()


if __name__ == '__main__':

    qc_boundaries_extracter = DownloadQcAdmBoundaries(geo_level = DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MRC)
    shp_mrc_raw = qc_boundaries_extracter.get_qc_administrative_boundaries()

    qc_mrc_dissolved_extracter = DownloadQcDissolvedAdmReg()
    shp_mrc = qc_mrc_dissolved_extracter.get_qc_administrative_boundaries()