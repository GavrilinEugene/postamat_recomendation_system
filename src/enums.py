# insert mapbox token (free)
token = 'pk.*******'

# insert gis token (not free)
key = 'demo'

# h3 index resolution
RES = 10

postgress_connection = ''
postgress_db = ''
lp = ''
schema = ''

bad_hex_with_postamat = ['8a11aa78c59ffff', '8a11aa783707fff', '8a11aa6a46e7fff',
       '8a11aa783057fff', '8a11aa7b595ffff', '8a11aa636357fff',
       '8a11aa78c737fff', '8a11aa70a6f7fff', '8a11aa781ceffff',
       '8a11aa794337fff', '8a11aa7a9b0ffff', '8a11aa71a98ffff',
       '8a11aa713807fff', '8a11aa7156c7fff', '8a11aa796687fff',
       '8a11aa612217fff', '8a11aa6adb2ffff', '8a11aa61e847fff',
       '8a11aa448d77fff', '8a11aa099d9ffff', '8a11aa723b07fff',
       '8a11aa70e8affff', '8a11aa7ac71ffff', '8a11aa776ba7fff',
       '8a11aa7ac41ffff', '8a11aa78c2c7fff', '8a11aa79099ffff',
       '8a11aa44329ffff', '8a11aa78290ffff', '8a11aa70a437fff',
       '8a11aa63146ffff', '8a11aa606c5ffff', '8a11aa7b049ffff',
       '8a11aa7a41b7fff', '8a11aa62eb2ffff', '8a11aa6b400ffff',
       '8a1181b602dffff', '8a11aa72b667fff', '8a1181b4c347fff',
       '8a11aa45a35ffff', '8a11aa619737fff', '8a11aa453d37fff',
       '8a11aa78c61ffff', '8a11aa62660ffff', '8a11aa7812cffff',
       '8a11aa6b6ba7fff', '8a11aa786527fff', '8a11aa4e9637fff',
       '8a11aa458537fff', '8a11aa6149b7fff', '8a11aa7a4347fff',
       '8a11aa702ceffff', '8a11aa4c8707fff', '8a1181b6a3affff',
       '8a1181b45117fff', '8a11aa6a3ca7fff', '8a11aa4dbc8ffff',
       '8a1181b76d47fff', '8a11aa798297fff', '8a11aa723b1ffff',
       '8a11aa7832dffff', '8a11aa7b30affff', '8a11aa6a0b2ffff',
       '8a11aa78daaffff', '8a11aa46bc87fff', '8a11aa794037fff',
       '8a11aa709687fff', '8a11aa6848a7fff', '8a11aa6b0b1ffff',
       '8a11aa6b44dffff', '8a11aa7994cffff', '8a11aa4c8d17fff',
       '8a11aa636747fff', '8a11aa71a28ffff', '8a11aa70b907fff',
       '8a11aa756427fff', '8a1181b6b9a7fff', '8a11aa781b4ffff',
       '8a11aa7aa937fff', '8a11aa70a087fff', '8a11aa633cc7fff',
       '8a11aa78dd97fff', '8a11aa781ac7fff', '8a11aa71e69ffff',
       '8a11aa7adc77fff', '8a11aa72a767fff', '8a11aa736d57fff',
       '8a11aa776d0ffff', '8a11aa7894affff', '8a11aa73689ffff',
       '8a11aa70505ffff', '8a11aa71632ffff', '8a11aa4daa67fff',
       '8a11aa7ae747fff', '8a11aa70a817fff', '8a11aa70d987fff',
       '8a11aa71d31ffff', '8a11aa6a68dffff', '8a11aa6a1d07fff',
       '8a11aa78c28ffff', '8a1181b6cd9ffff', '8a11aa62e9b7fff',
       '8a11aa710c67fff', '8a11aa799247fff', '8a11aa7116b7fff',
       '8a11aa6a4a6ffff', '8a1181b6ec77fff', '8a11aa791c2ffff',
       '8a11aa4da8cffff', '8a11aa7adb47fff', '8a11aa6a344ffff',
       '8a11aa6853b7fff', '8a11aa788107fff']