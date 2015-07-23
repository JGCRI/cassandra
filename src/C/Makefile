OPT    =  -O3 -g
CFLAGS = $(OPT) -I$(NETCDF_INCDIR) -std=c99 
LDFLAGS = -L$(NETCDF_LIBDIR) -Wl,-rpath,$(NETCDF_LIBDIR)
LIBS   = -lnetcdf -lm


mat2nc: mat2nc.o
	gcc $(LDFLAGS) -o mat2nc mat2nc.o $(LIBS)
