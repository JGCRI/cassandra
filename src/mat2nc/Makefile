CXXFLAGS = -O3 -ffast-math -I$(NETCDF_INCDIR)
LIBS   = -lnetcdf


mat2nc: mat2nc.o
	g++ -o mat2nc mat2nc.o -L$(NETCDF_LIBDIR) -Wl,-rpath,$(NETCDF_LIBDIR)  $(LIBS)

