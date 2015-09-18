def pplnt_grid(tuple_list, resolution=[.5, .5], boundary= [-180, -90, 180, 90]):
    """Takes a list of (lon, lat, water_usage) tuples ('tuple_list') and returns dictionary with tuples assigned
    to a grid cell with key (i,j).
    
    Resolution: (i, j) dimensions of individual grid cells.
    Boundary: (lon0, lat0, lon1, lat1) where(lon0,lat0) are (lon, lat) coordinates of lower left corner of grid
    and (lon1, lat1) are (lon, lat) coordinates of upper right corner of grid."""

    #Initialize grid dictionary
    grid = {}
    
    #Get dimensions of grid cell
    i_dim = resolution[0]  #length of side i
    j_dim = resolution[1]  #length of side j

    #Map (lon,lat) dimensions to (i,j) dimensions
    lon_start = boundary[0] 
    lat_start = boundary[1]
    lon_end = boundary[2]
    lat_end = boundary[3]

    i_start = 0 
    j_start = 0 

    lon_offset = abs(lon_start - i_start)
    lat_offset = abs(lat_start - j_start)

    i_end = int((lon_end + lon_offset)/i_dim) #Note: will truncate end of lon,lat if not evenly divisible by i_dim, j_dim
    j_end = int((lat_end + lat_offset)/j_dim)
    
    #Add (lon, lat, water_usage) tuple to grid using (i, j) key
    for n in range(0, len(tuple_list)):
        #Water usage
        water = tuple_list[n][2]
        
        #Calculate (i,j) values
        #Ex: lon values of -180 to -179.51 are 0, -179.5 to -179.01 are 1, etc.
        i = int((tuple_list[n][0]+lon_offset)/i_dim)
        j = int((tuple_list[n][1]+lat_offset)/j_dim)

        #Reassign points on upper boundaries to previous grid cell
        if i==i_end:
            i = i_end - 1
        if j==j_end:
            j = j_end-1

        #Only add cells to dictionary if within grid boundaries. Include edges. Print warning message if not.
        #For existing cells, add new water usage value to total
        if (i>=i_start and i<i_end) and (j>=j_start and j<j_end):
            if (i,j) in grid:
                grid[(i,j)] = grid[(i,j)] + water
            else:
                grid[(i,j)]= []
                grid[(i,j)].append(water)
        else:
            print("(%d, %d) is located outside of the grid boundary and will be excluded." %(i, j))
    
    return(grid)






