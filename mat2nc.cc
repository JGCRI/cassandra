#include <fstream>
#include <iostream>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <netcdf.h>
#include <string>

/* Assume the grid is always 360 by 720 */ 
#define NLAT 360
#define NLON 720

/* contents of the config file:
 *
 * input filename
 * output filename
 * variable name
 * variable unit
 * time unit (e.g. years)
 * start time coordinate
 * time coordinate increment
 * number of time slices
 * number of time slices per time unit
 *
 * Example, to output monthly data at yearly intervals, starting in 2006:
 *
 * foo.dat
 * foo.nc
 * km^3
 * natural_streamflow
 * year
 * 2006
 * 1
 * 1140
 * 12
 */

struct datainfo {
  std::string infile;
  std::string outfile;
  std::string varname;
  std::string varunit;
  std::string timeunit;
  int timestart;
  int timeinc;
  int ntin;
  int ntavg;                  // number of time slices to average into a single output slice
  /* Everything above here is read from the input file */
  int ntot;                     // total number of time slices in output (calculated from the above)
};

  
/* array of NLAT x NLON matrices */
typedef float dataslice[NLAT][NLON];
typedef dataslice *datasliceptr;
typedef float transslice[NLON*NLAT]; // transposed, flattened slice
typedef transslice *transsliceptr;


int parseconfig(char *filename, struct datainfo *datinf);
int read_and_aggregate_data(const struct datainfo *datinf, dataslice data[]);
int mat2nc(const struct datainfo *datinf, const float data[]);

int main(int argc, char *argv[])
{
  struct datainfo datinf;
  datasliceptr data;
  int rc;

  if(argc != 2) {
    std::cerr << "Usage: " << argv[0] << " configfile\n";
    return 1;
  } 

  rc = parseconfig(argv[1], &datinf);
  if(rc != 0) {
    std::cerr << "Error parsing config file.\n";
    return rc;
  }

  data = new dataslice[datinf.ntot];
  if(!data) {
    std::cerr << "Unable to allocate " << datinf.ntot*sizeof(dataslice) << " bytes for data.\n";
    return 2;
  }

  rc = read_and_aggregate_data(&datinf, data);
  if(rc!=0) {
    std::cerr << "Error reading input data.\n";
    return rc;
  }

  rc = mat2nc(&datinf, (float *)data);
  if(rc != 0) {
    std::cerr << "Error writing netcdf file.\n";
    return rc;
  }

  std::cerr << "Success.  Output file is " << datinf.outfile << "\n";

  delete [] data;
  
  return 0;
}

int parseconfig(char *filename, struct datainfo *datinf)
{
  std::ifstream infile(filename);

  if(!infile) {
    std::cerr << "Unable to open file '" << filename << "' for input.\n";
    return 4;
  }

  infile >> datinf->infile;
  infile >> datinf->outfile;
  infile >> datinf->varname;
  infile >> datinf->varunit;
  infile >> datinf->timeunit;
  infile >> datinf->timestart;
  infile >> datinf->timeinc;
  infile >> datinf->ntin;
  infile >> datinf->ntavg;
  datinf->ntot = datinf->ntin / datinf->ntavg; // drop any incomplete averaging periods.

  return 0;
}

int read_and_aggregate_data(const struct datainfo *datinf, dataslice data[])
{
  FILE *infile = fopen(datinf->infile.c_str(), "rb");
  if(!infile) {
    std::cerr << "Unable to open file " << datinf->infile << " for input.\n";
    return 4;
  }

  /* data is data[datinf->ntot][NLAT][NLON]; */
  /* Input data is organized as data[t][lon][lat], so we need buffers to reorganize it.*/
  transsliceptr readdata = new transslice[datinf->ntavg]; // read buffer
  transslice avgdata;           // transpose buffer
  
  for(int ichunk=0; ichunk < datinf->ntot; ++ichunk) {
    size_t nread = fread((void*)readdata, sizeof(float), datinf->ntavg*NLAT*NLON, infile);
    if(nread != datinf->ntavg*NLAT*NLON) {
      std::cerr << "Error reading data from " << datinf->infile << " at chunk " << ichunk << "\n";
      return 5;
    }

    // Aggregate all of the time slices in the chunk
    memset(avgdata, 0, NLAT*NLON*sizeof(float));
    for(int islice=0; islice < datinf->ntavg; ++islice)
      for(int i=0; i<NLAT*NLON; ++i)
        avgdata[i] += readdata[islice][i];

    // transpose the aggregated data
    for(int ilon=0; ilon<NLON; ++ilon)
      for(int ilat=0; ilat<NLAT; ++ilat)
        data[ichunk][ilat][ilon] = avgdata[NLAT*ilon + ilat];
  }

  delete [] readdata;
  fclose(infile);
}

void
check_err(const int stat, const int line, const char *file) {
    if (stat != NC_NOERR) {
        (void)fprintf(stderr,"line %d of %s: %s\n", line, file, nc_strerror(stat));
        fflush(stderr);
        exit(1);
    }
}


int mat2nc(const struct datainfo *datinf, const float data[])
{
  const char *outfile = datinf->outfile.c_str();

  int stat;                     // return status
  int ncid;                     // netCDF id

  /* dimension ids */
  int lat_dim;
  int lon_dim;
  int time_dim;
  
  /* dimension lengths */
  size_t lat_len = NLAT;
  size_t lon_len = NLON;
  size_t time_len = datinf->ntot;
  
  /* variable ids */
  int lat_id;
  int lon_id;
  int time_id;
  int data_id;

  /* variable shapes -- omit one-dimensional vars */
  int data_dims[3];

  /* enter define mode */
  stat = nc_create(outfile, NC_CLOBBER, &ncid);
  check_err(stat,__LINE__,__FILE__);

  /* define dimensions */
  stat = nc_def_dim(ncid, "lat", lat_len, &lat_dim);
  check_err(stat,__LINE__,__FILE__);
  stat = nc_def_dim(ncid, "lon", lon_len, &lon_dim);
  check_err(stat,__LINE__,__FILE__);
  stat = nc_def_dim(ncid, "time", time_len, &time_dim);
  check_err(stat,__LINE__,__FILE__);

  /* define variables */
  
  stat = nc_def_var(ncid, "lat", NC_FLOAT, 1, &lat_dim, &lat_id);
  check_err(stat,__LINE__,__FILE__);

  stat = nc_def_var(ncid, "lon", NC_FLOAT, 1, &lon_dim, &lon_id);
  check_err(stat,__LINE__,__FILE__);

  stat = nc_def_var(ncid, "time", NC_FLOAT, 1, &time_dim, &time_id);
  check_err(stat,__LINE__,__FILE__);

  data_dims[0] = time_dim;
  data_dims[1] = lat_dim;
  data_dims[2] = lon_dim;
  stat = nc_def_var(ncid, datinf->varname.c_str(), NC_FLOAT, 3, data_dims, &data_id);
  check_err(stat,__LINE__,__FILE__);

  /* assign per-variable attributes */
  stat = nc_put_att_text(ncid, lat_id, "units", 13, "degrees_north");
  check_err(stat,__LINE__,__FILE__);
  
  stat = nc_put_att_text(ncid, lon_id, "units", 12, "degrees_east");
  check_err(stat,__LINE__,__FILE__);
    
  size_t unitlen = datinf->timeunit.length();
  stat = nc_put_att_text(ncid, time_id, "units", unitlen, datinf->timeunit.c_str());
  check_err(stat,__LINE__,__FILE__);

  unitlen = datinf->varunit.length();
  stat = nc_put_att_text(ncid, data_id, "units", unitlen, datinf->varunit.c_str());
  check_err(stat,__LINE__,__FILE__);

  /* leave define mode */
  stat = nc_enddef (ncid);
  check_err(stat,__LINE__,__FILE__);

      /* assign variable data (this was generated by ncgen) */
  {
    float lat_data[NLAT] = {-89.75, -89.25, -88.75, -88.25, -87.75, -87.25, -86.75, -86.25, -85.75, -85.25, -84.75, -84.25, -83.75, -83.25, -82.75, -82.25, -81.75, -81.25, -80.75, -80.25, -79.75, -79.25, -78.75, -78.25, -77.75, -77.25, -76.75, -76.25, -75.75, -75.25, -74.75, -74.25, -73.75, -73.25, -72.75, -72.25, -71.75, -71.25, -70.75, -70.25, -69.75, -69.25, -68.75, -68.25, -67.75, -67.25, -66.75, -66.25, -65.75, -65.25, -64.75, -64.25, -63.75, -63.25, -62.75, -62.25, -61.75, -61.25, -60.75, -60.25, -59.75, -59.25, -58.75, -58.25, -57.75, -57.25, -56.75, -56.25, -55.75, -55.25, -54.75, -54.25, -53.75, -53.25, -52.75, -52.25, -51.75, -51.25, -50.75, -50.25, -49.75, -49.25, -48.75, -48.25, -47.75, -47.25, -46.75, -46.25, -45.75, -45.25, -44.75, -44.25, -43.75, -43.25, -42.75, -42.25, -41.75, -41.25, -40.75, -40.25, -39.75, -39.25, -38.75, -38.25, -37.75, -37.25, -36.75, -36.25, -35.75, -35.25, -34.75, -34.25, -33.75, -33.25, -32.75, -32.25, -31.75, -31.25, -30.75, -30.25, -29.75, -29.25, -28.75, -28.25, -27.75, -27.25, -26.75, -26.25, -25.75, -25.25, -24.75, -24.25, -23.75, -23.25, -22.75, -22.25, -21.75, -21.25, -20.75, -20.25, -19.75, -19.25, -18.75, -18.25, -17.75, -17.25, -16.75, -16.25, -15.75, -15.25, -14.75, -14.25, -13.75, -13.25, -12.75, -12.25, -11.75, -11.25, -10.75, -10.25, -9.75, -9.25, -8.75, -8.25, -7.75, -7.25, -6.75, -6.25, -5.75, -5.25, -4.75, -4.25, -3.75, -3.25, -2.75, -2.25, -1.75, -1.25, -0.75, -0.25, 0.25, 0.75, 1.25, 1.75, 2.25, 2.75, 3.25, 3.75, 4.25, 4.75, 5.25, 5.75, 6.25, 6.75, 7.25, 7.75, 8.25, 8.75, 9.25, 9.75, 10.25, 10.75, 11.25, 11.75, 12.25, 12.75, 13.25, 13.75, 14.25, 14.75, 15.25, 15.75, 16.25, 16.75, 17.25, 17.75, 18.25, 18.75, 19.25, 19.75, 20.25, 20.75, 21.25, 21.75, 22.25, 22.75, 23.25, 23.75, 24.25, 24.75, 25.25, 25.75, 26.25, 26.75, 27.25, 27.75, 28.25, 28.75, 29.25, 29.75, 30.25, 30.75, 31.25, 31.75, 32.25, 32.75, 33.25, 33.75, 34.25, 34.75, 35.25, 35.75, 36.25, 36.75, 37.25, 37.75, 38.25, 38.75, 39.25, 39.75, 40.25, 40.75, 41.25, 41.75, 42.25, 42.75, 43.25, 43.75, 44.25, 44.75, 45.25, 45.75, 46.25, 46.75, 47.25, 47.75, 48.25, 48.75, 49.25, 49.75, 50.25, 50.75, 51.25, 51.75, 52.25, 52.75, 53.25, 53.75, 54.25, 54.75, 55.25, 55.75, 56.25, 56.75, 57.25, 57.75, 58.25, 58.75, 59.25, 59.75, 60.25, 60.75, 61.25, 61.75, 62.25, 62.75, 63.25, 63.75, 64.25, 64.75, 65.25, 65.75, 66.25, 66.75, 67.25, 67.75, 68.25, 68.75, 69.25, 69.75, 70.25, 70.75, 71.25, 71.75, 72.25, 72.75, 73.25, 73.75, 74.25, 74.75, 75.25, 75.75, 76.25, 76.75, 77.25, 77.75, 78.25, 78.75, 79.25, 79.75, 80.25, 80.75, 81.25, 81.75, 82.25, 82.75, 83.25, 83.75, 84.25, 84.75, 85.25, 85.75, 86.25, 86.75, 87.25, 87.75, 88.25, 88.75, 89.25, 89.75} ;
    size_t lat_startset[1] = {0} ;
    size_t lat_countset[1] = {NLAT} ;
    stat = nc_put_vara(ncid, lat_id, lat_startset, lat_countset, lat_data);
    check_err(stat,__LINE__,__FILE__);
  }

  {
    float lon_data[NLON] = {-179.75, -179.25, -178.75, -178.25, -177.75, -177.25, -176.75, -176.25, -175.75, -175.25, -174.75, -174.25, -173.75, -173.25, -172.75, -172.25, -171.75, -171.25, -170.75, -170.25, -169.75, -169.25, -168.75, -168.25, -167.75, -167.25, -166.75, -166.25, -165.75, -165.25, -164.75, -164.25, -163.75, -163.25, -162.75, -162.25, -161.75, -161.25, -160.75, -160.25, -159.75, -159.25, -158.75, -158.25, -157.75, -157.25, -156.75, -156.25, -155.75, -155.25, -154.75, -154.25, -153.75, -153.25, -152.75, -152.25, -151.75, -151.25, -150.75, -150.25, -149.75, -149.25, -148.75, -148.25, -147.75, -147.25, -146.75, -146.25, -145.75, -145.25, -144.75, -144.25, -143.75, -143.25, -142.75, -142.25, -141.75, -141.25, -140.75, -140.25, -139.75, -139.25, -138.75, -138.25, -137.75, -137.25, -136.75, -136.25, -135.75, -135.25, -134.75, -134.25, -133.75, -133.25, -132.75, -132.25, -131.75, -131.25, -130.75, -130.25, -129.75, -129.25, -128.75, -128.25, -127.75, -127.25, -126.75, -126.25, -125.75, -125.25, -124.75, -124.25, -123.75, -123.25, -122.75, -122.25, -121.75, -121.25, -120.75, -120.25, -119.75, -119.25, -118.75, -118.25, -117.75, -117.25, -116.75, -116.25, -115.75, -115.25, -114.75, -114.25, -113.75, -113.25, -112.75, -112.25, -111.75, -111.25, -110.75, -110.25, -109.75, -109.25, -108.75, -108.25, -107.75, -107.25, -106.75, -106.25, -105.75, -105.25, -104.75, -104.25, -103.75, -103.25, -102.75, -102.25, -101.75, -101.25, -100.75, -100.25, -99.75, -99.25, -98.75, -98.25, -97.75, -97.25, -96.75, -96.25, -95.75, -95.25, -94.75, -94.25, -93.75, -93.25, -92.75, -92.25, -91.75, -91.25, -90.75, -90.25, -89.75, -89.25, -88.75, -88.25, -87.75, -87.25, -86.75, -86.25, -85.75, -85.25, -84.75, -84.25, -83.75, -83.25, -82.75, -82.25, -81.75, -81.25, -80.75, -80.25, -79.75, -79.25, -78.75, -78.25, -77.75, -77.25, -76.75, -76.25, -75.75, -75.25, -74.75, -74.25, -73.75, -73.25, -72.75, -72.25, -71.75, -71.25, -70.75, -70.25, -69.75, -69.25, -68.75, -68.25, -67.75, -67.25, -66.75, -66.25, -65.75, -65.25, -64.75, -64.25, -63.75, -63.25, -62.75, -62.25, -61.75, -61.25, -60.75, -60.25, -59.75, -59.25, -58.75, -58.25, -57.75, -57.25, -56.75, -56.25, -55.75, -55.25, -54.75, -54.25, -53.75, -53.25, -52.75, -52.25, -51.75, -51.25, -50.75, -50.25, -49.75, -49.25, -48.75, -48.25, -47.75, -47.25, -46.75, -46.25, -45.75, -45.25, -44.75, -44.25, -43.75, -43.25, -42.75, -42.25, -41.75, -41.25, -40.75, -40.25, -39.75, -39.25, -38.75, -38.25, -37.75, -37.25, -36.75, -36.25, -35.75, -35.25, -34.75, -34.25, -33.75, -33.25, -32.75, -32.25, -31.75, -31.25, -30.75, -30.25, -29.75, -29.25, -28.75, -28.25, -27.75, -27.25, -26.75, -26.25, -25.75, -25.25, -24.75, -24.25, -23.75, -23.25, -22.75, -22.25, -21.75, -21.25, -20.75, -20.25, -19.75, -19.25, -18.75, -18.25, -17.75, -17.25, -16.75, -16.25, -15.75, -15.25, -14.75, -14.25, -13.75, -13.25, -12.75, -12.25, -11.75, -11.25, -10.75, -10.25, -9.75, -9.25, -8.75, -8.25, -7.75, -7.25, -6.75, -6.25, -5.75, -5.25, -4.75, -4.25, -3.75, -3.25, -2.75, -2.25, -1.75, -1.25, -0.75, -0.25, 0.25, 0.75, 1.25, 1.75, 2.25, 2.75, 3.25, 3.75, 4.25, 4.75, 5.25, 5.75, 6.25, 6.75, 7.25, 7.75, 8.25, 8.75, 9.25, 9.75, 10.25, 10.75, 11.25, 11.75, 12.25, 12.75, 13.25, 13.75, 14.25, 14.75, 15.25, 15.75, 16.25, 16.75, 17.25, 17.75, 18.25, 18.75, 19.25, 19.75, 20.25, 20.75, 21.25, 21.75, 22.25, 22.75, 23.25, 23.75, 24.25, 24.75, 25.25, 25.75, 26.25, 26.75, 27.25, 27.75, 28.25, 28.75, 29.25, 29.75, 30.25, 30.75, 31.25, 31.75, 32.25, 32.75, 33.25, 33.75, 34.25, 34.75, 35.25, 35.75, 36.25, 36.75, 37.25, 37.75, 38.25, 38.75, 39.25, 39.75, 40.25, 40.75, 41.25, 41.75, 42.25, 42.75, 43.25, 43.75, 44.25, 44.75, 45.25, 45.75, 46.25, 46.75, 47.25, 47.75, 48.25, 48.75, 49.25, 49.75, 50.25, 50.75, 51.25, 51.75, 52.25, 52.75, 53.25, 53.75, 54.25, 54.75, 55.25, 55.75, 56.25, 56.75, 57.25, 57.75, 58.25, 58.75, 59.25, 59.75, 60.25, 60.75, 61.25, 61.75, 62.25, 62.75, 63.25, 63.75, 64.25, 64.75, 65.25, 65.75, 66.25, 66.75, 67.25, 67.75, 68.25, 68.75, 69.25, 69.75, 70.25, 70.75, 71.25, 71.75, 72.25, 72.75, 73.25, 73.75, 74.25, 74.75, 75.25, 75.75, 76.25, 76.75, 77.25, 77.75, 78.25, 78.75, 79.25, 79.75, 80.25, 80.75, 81.25, 81.75, 82.25, 82.75, 83.25, 83.75, 84.25, 84.75, 85.25, 85.75, 86.25, 86.75, 87.25, 87.75, 88.25, 88.75, 89.25, 89.75, 90.25, 90.75, 91.25, 91.75, 92.25, 92.75, 93.25, 93.75, 94.25, 94.75, 95.25, 95.75, 96.25, 96.75, 97.25, 97.75, 98.25, 98.75, 99.25, 99.75, 100.25, 100.75, 101.25, 101.75, 102.25, 102.75, 103.25, 103.75, 104.25, 104.75, 105.25, 105.75, 106.25, 106.75, 107.25, 107.75, 108.25, 108.75, 109.25, 109.75, 110.25, 110.75, 111.25, 111.75, 112.25, 112.75, 113.25, 113.75, 114.25, 114.75, 115.25, 115.75, 116.25, 116.75, 117.25, 117.75, 118.25, 118.75, 119.25, 119.75, 120.25, 120.75, 121.25, 121.75, 122.25, 122.75, 123.25, 123.75, 124.25, 124.75, 125.25, 125.75, 126.25, 126.75, 127.25, 127.75, 128.25, 128.75, 129.25, 129.75, 130.25, 130.75, 131.25, 131.75, 132.25, 132.75, 133.25, 133.75, 134.25, 134.75, 135.25, 135.75, 136.25, 136.75, 137.25, 137.75, 138.25, 138.75, 139.25, 139.75, 140.25, 140.75, 141.25, 141.75, 142.25, 142.75, 143.25, 143.75, 144.25, 144.75, 145.25, 145.75, 146.25, 146.75, 147.25, 147.75, 148.25, 148.75, 149.25, 149.75, 150.25, 150.75, 151.25, 151.75, 152.25, 152.75, 153.25, 153.75, 154.25, 154.75, 155.25, 155.75, 156.25, 156.75, 157.25, 157.75, 158.25, 158.75, 159.25, 159.75, 160.25, 160.75, 161.25, 161.75, 162.25, 162.75, 163.25, 163.75, 164.25, 164.75, 165.25, 165.75, 166.25, 166.75, 167.25, 167.75, 168.25, 168.75, 169.25, 169.75, 170.25, 170.75, 171.25, 171.75, 172.25, 172.75, 173.25, 173.75, 174.25, 174.75, 175.25, 175.75, 176.25, 176.75, 177.25, 177.75, 178.25, 178.75, 179.25, 179.75} ;
    size_t lon_startset[1] = {0} ;
    size_t lon_countset[1] = {NLON} ;
    stat = nc_put_vara(ncid, lon_id, lon_startset, lon_countset, lon_data);
    check_err(stat,__LINE__,__FILE__);
  }

  float *time_data = new float[datinf->ntot];
  for(int i=0; i<datinf->ntot; ++i)
    time_data[i] = datinf->timestart + i*datinf->timeinc;
  stat = nc_put_var(ncid, time_id, time_data);
  check_err(stat,__LINE__,__FILE__);

  stat = nc_put_var_float(ncid, data_id, data);
  check_err(stat,__LINE__,__FILE__);

  stat = nc_close(ncid);
  check_err(stat,__LINE__,__FILE__);
  
  delete [] time_data;
  
  return 0;
}
