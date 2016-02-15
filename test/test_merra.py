import unittest
import wdata.merra as m
from datetime import date

class TestCreateURL(unittest.TestCase):
    def test_merra_wind(self):
        settings = m.PRESETS['merra']
        url,label = m.create_url(date(1979,1,1),'wind',settings=settings,filefmt='hdf',revision=0,bbox=(30,-15,75,42.5))
        correct_url = "http://goldsmr2.sci.gsfc.nasa.gov/daac-bin/OTF/HTTP_services.cgi?VERSION=1.02&BBOX=30%2C-15%2C75%2C42.5&SERVICE=SUBSET_LATS4D&FORMAT=SERGLw&VARIABLES=v2m%2Cv10m%2Cv50m%2Cu2m%2Cu10m%2Cu50m%2Cdisph&LABEL=MERRA100.prod.assim.tavg1_2d_slv_Nx.19790101.SUB.hdf&SHORTNAME=MAT1NXSLV&FILENAME=%2Fdata%2Fs4pa%2FMERRA%2FMAT1NXSLV.5.2.0%2F1979%2F01%2FMERRA100.prod.assim.tavg1_2d_slv_Nx.19790101.hdf"
        self.assertEqual(url, correct_url)
        
    def test_merra_solar(self):
        settings = m.PRESETS['merra']
        url,label = m.create_url(date(1979,1,1),'solar',settings=settings,filefmt='hdf',revision=0,bbox=(30,-15,75,42.5))
        correct_url = 'http://goldsmr2.sci.gsfc.nasa.gov/daac-bin/OTF/HTTP_services.cgi?VERSION=1.02&BBOX=30%2C-15%2C75%2C42.5&SERVICE=SUBSET_LATS4D&FORMAT=SERGLw&VARIABLES=ts%2Calbedo%2Calbvisdf%2Calbvisdr%2Cswtdn%2Cswgdn&LABEL=MERRA100.prod.assim.tavg1_2d_rad_Nx.19790101.SUB.hdf&SHORTNAME=MAT1NXRAD&FILENAME=%2Fdata%2Fs4pa%2FMERRA%2FMAT1NXRAD.5.2.0%2F1979%2F01%2FMERRA100.prod.assim.tavg1_2d_rad_Nx.19790101.hdf'
        self.assertEqual(url, correct_url)