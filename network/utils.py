
import pandas as pd
import geopandas as gpd
from tqdm.notebook import tqdm
tqdm.pandas()
from shapely.geometry import Point, MultiPoint, Polygon, LineString, MultiLineString
from shapely.ops import nearest_points, split, linemerge,transform
from shapely.geometry import mapping
from shapely.geometry import shape
from shapely.wkt import loads
from rtree import index


def lines_and_interpolated_vertices(data,chunk,kept_attributes):
    
    mod_dataset = []
    re_mod_dataset = []
    for row in tqdm(data.iterrows()):
        lin = row[1]['geometry']
        line_attributes = row[1][kept_attributes].to_dict()
        vertices = MultiPoint(list(lin.coords))
        coord = tuple(lin.coords)
        
        segments ={}
        for position in range(len(coord)-1):
            segment = {position+1:LineString([Point(coord[position]),Point(coord[position+1])]).wkt}
            segments.update(segment)

        heights = (max(coord[0][2],coord[-1][2]),min(coord[0][2],coord[-1][2]))
        height = heights[0]-heights[1]
        start = [tup for tup in coord if heights[1] in tup][0]
        end = [tup for tup in coord if heights[0] in tup][0]
        long = lin.length
        slope = height/long
        if long < chunk+20:
            parts = 1
        if long >= chunk+20:
            parts = round(long/chunk)
        slice_dist = long / parts
        
        interpolated =[]
        if parts != 1:

            for i in range(parts):
                npoint = lin.interpolate(slice_dist*i)
                interpolated.append(npoint)
            interpolated = interpolated[1:]
            
            result =[]
            for segment in segments:
                segment_line = LineString(loads(segments[segment]))
                segments[segment] = segment_line
                for p in interpolated:
                    if segment_line.distance(p) < 1e-8:
                        segments[segment] = LineString([Point(segment_line.coords[0]),p,Point(segment_line.coords[1])])
                    else:
                        continue     

            modified_line = MultiLineString([s for s in segments.values()])
            modified_line = linemerge (modified_line)
            breaks = MultiPoint(interpolated)
            new_lines = list(split(modified_line,breaks))

        else:
            modified_line = lin
            new_lines = [modified_line]
            #print("Not Broken")
        
        new_data = []
        mod_original = []
        for new_line in new_lines:
            attributes = line_attributes
            attr = {'slope':slope,'length':new_line.length,'geometry':new_line.wkt}
            attr.update(attributes)
            new_data.append(attr)

        attributes = line_attributes
        attr = {'slope':slope,'length':modified_line.length,'geometry':modified_line.wkt}
        attr.update(attributes)
        mod_original.append(attr)
        
        mod_dataset.append(pd.DataFrame(new_data))
        re_mod_dataset.append(pd.DataFrame(mod_original))
    
    #The split dataset
    fin1 = pd.concat(mod_dataset)
    #the original dataset with added vertices
    fin2 = pd.concat(re_mod_dataset)
    return (fin1,fin2)

def split_lines_at_points(data,points,kept_attributes,id_field):
    #Note that vertices have to exist in the geometry

    points = MultiPoint([Point(loads(p)) for p in points])
    mod_dataset = []
    for row in tqdm(data.iterrows()):
        if row[1]['geometry'].startswith('MULTILINESTRING Z'):
            continue
        else:
            lin = LineString(loads(row[1]['geometry']))
        line_attributes = row[1][kept_attributes].to_dict()
        vertices = MultiPoint(list(lin.coords))
        coord = list(lin.coords)
        heights = (max(coord[0][2],coord[-1][2]),min(coord[0][2],coord[-1][2]))
        height = heights[0]-heights[1]
        start = [tup for tup in coord if heights[1] in tup][0]
        end = [tup for tup in coord if heights[0] in tup][0]
        long = lin.length
        slope = height/long
        inner = MultiPoint([Point(p) for p in coord[1:-1] if Point(p).distance(points) < 1e-8])
        new_data = []

        if inner.is_empty:
            new_lines = [lin]
            pass
        else:
            new_lines = list(split(lin,inner))
        for new_line in new_lines:
            attributes = line_attributes
            attr = {'slope':slope,'length':new_line.length,'geometry':new_line.wkt}
            attr.update(attributes)
            new_data.append(attr)

        mod_dataset.append(pd.DataFrame(new_data))

    split_df = pd.concat(mod_dataset)
    return(split_df)

def get_lines_endpoints(data):
    points = []
    for row in tqdm(data.iterrows()):
        lin = loads(row[1]['geometry'])
        l = list(lin.coords)
        ends = [l[0],l[-1]]
        points += ends
    points = list(dict.fromkeys(points))
    points = [Point(p) for p in points]
    return (points)

def find_nearest(data,data_near,near_id):

    idx = rtree.index.Index()

    for fid, row in tqdm(data_near.iterrows()):
        geometry = row['geometry'].bounds
        idf = row[near_id]
        idx.insert(fid, geometry)
    
   
    adjacency = {}
    
    for fid, row in tqdm(data.iterrows()):
        bpoint = row['geometry'].bounds
        near_polys = {data_near.iloc[f][near_id]:data_near.iloc[f]['geometry'] for f in idx.nearest(bpoint, 1)}
        point = Point(row['geometry'])
        min_distance, min_poly = min(((poly.distance(point), poly) for poly in near_polys.values()), key=itemgetter(0))
        closest_poly = list({k:v for k,v in near_polys.items() if v == min_poly}.keys())[0]
        result = {fid:closest_poly}
        adjacency.update(result)

    adjacency = pd.DataFrame.from_dict(adjacency,orient='index',columns=[near_id])
    
    fdata = pd.merge(data,adjacency,how='left',left_index=True,right_index= True).reset_index()
    
    return fdata

#Ahora estamos probando con 4 para ajustar mas
def tobler_speed_up(length,slope):
    #enters in radians and meters, gives minutes
    return round(4*(math.exp(-3.5*abs(slope+0.05)))*16.67,2)
def tobler_speed_down(length,slope):
    #enters in radians and meters, gives minutes   
    return round(4*(math.exp(-3.5*abs(-(slope)+0.05)))*16.67,2)

def calculate_speed_by_slope(network,length_attribute,slope_attribute):
    network['speed_up'] = network.apply(lambda row: tobler_speed_up (row[length_attribute],row[slope_attribute]),axis=1)
    network['speed_down'] = network.apply(lambda row: tobler_speed_down (row[length_attribute],row[slope_attribute]),axis=1)
    network['time_up'] = network[length_attribute] / network['speed_up']
    network['time_down'] = network[length_attribute] / network['speed_down']
    return(network)

def remove_z_line(data_z):
    for i,row in tqdm(data_z.iterrows()):
        a = [coordinate[:2] for coordinate in list(row['geometry'].coords)]
        data_z.at[i,'geometry'] = LineString(a)
    return data_z
def remove_z_point(data_z):
    for i,row in tqdm(data_z.iterrows()):
        #print(row['geometry'].coords[0][:2])
        data_z.at[i,'geometry'] = Point(row['geometry'].coords[0][:2])
    return data_z

def create_undirected_from_shp(shape_edges,shape_nodes,node_id_field,edge_id_field,weight_field,project_name):
    
    print("\nGenerating Graph...")
    E = nx.read_shp(shape_edges, simplify = True , geom_attrs=True)
    N = nx.read_shp(shape_nodes, simplify = False)
    G = nx.Graph()
    G = G.to_undirected()
    print("\nLoading Edge attributes")
    for data in tqdm(E.edges(data=True)):
        if data[1]:
            G.add_edge(data[0],data[1],ID=data[2][edge_id_field],weight=data[2][weight_field])
    print("\nLoading Node attributes")
    for data in tqdm(N.nodes(data=True)):
        if data[1]:
            G.add_node(data[0],attr_dict=data[1])
    d = {k:v['attr_dict'][node_id_field] for k,v in dict(G.nodes(data=True)).items()}
    G = nx.relabel_nodes(G, d, copy=False)
    print("\nGraph Ready")
    return G