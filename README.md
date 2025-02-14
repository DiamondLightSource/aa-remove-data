[![CI](https://github.com/DiamondLightSource/aa-remove-data/actions/workflows/ci.yml/badge.svg)](https://github.com/DiamondLightSource/aa-remove-data/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/DiamondLightSource/aa-remove-data/branch/main/graph/badge.svg)](https://codecov.io/gh/DiamondLightSource/aa-remove-data)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

# aa_remove_data

This repository contains tools that can be used to learn about, and reduce the size of, PV data 
contained in the Epics Archiver Appliance. There are a series of commands which apply 
different algorithms to files containing this data. Archiver Appliance data is stored in 
the Protocol Buffers (PB) format, which you can read more about here: [protobuf.dev](https://protobuf.dev/). 
This is a serialised data format - to access the data, which is necessary for some algorithms, 
the data must first be deserialised. The ArchiverData class handles the reading, writing, 
serialising and deserialising of PB data.

The tools are split up into two apps, **pb-tools** and **aa-remove-data**.

Source          | <https://github.com/DiamondLightSource/aa-remove-data>
:---:           | :---:
Docker          | `docker run ghcr.io/diamondlightsource/aa-remove-data:latest`
Releases        | <https://github.com/DiamondLightSource/aa-remove-data/releases>

pb-tools
--------

- **print-header** *filename* *\[options]*

*Print the header and a few lines of an archiver appliance PB file.*
```
pb-tools print-header pb_data/RAW:2025.pb --lines 5
Name: BL11K-EA-ADC-01:M4:CH4:RAW, Type: SCALAR_INT, Year: 2025
DATE                   SECONDS     NANO         VAL
2025-01-01 00:00:00           0      2588941    -1850
2025-01-01 00:00:00           0    102596158    -2544
2025-01-01 00:00:00           0    202583899    -2351
2025-01-01 00:00:00           0    302584993    -1824
2025-01-01 00:00:00           0    402589933    -3447
```
- **pb-2-txt** *filename* *txt-filename*

*Convert an archiver appliance PB file to a human readable text file.*
```
pb-tools pb-2-txt pb_data/RAW:2025.pb
```

aa-remove-data
--------------

- **to-period** *filename* *period* *\[options]*

*Reduce the frequency of data in a PB file by setting a minimum period between data points.*
```
aa-remove-data to-period pb_data/RAW:2025.pb 10
```
- **by-factor** *filename* *factor* *\[options]*

*Reduce the number of data points in a PB file by a certain factor.*
```
aa-remove-data by-factor pb_data/RAW:2025.pb 3
```

- **remove-before** *filename* *timestamp* *\[options]*

*Remove all data points in a PB file before a certain timestamp.*
```
aa-remove-data remove-before pb_data/RAW:2025.pb 1,2,3,4
```

- **remove-after** *filename* *timestamp* *\[options]*

*Remove all data points in a PB file after a certain timestamp.*
```
aa-remove-data remove-before pb_data/RAW:2025.pb 1,2,3,4
```


EPICS Archiver Appliance PB file structure
==========================================

EPICS Archiver Appliance data is structured with one sample per line of a PB file. The first line is
a header that contains information about the PV the file belongs to. Additionally, it contains the year
the data was collected. There is one PB file per PV per year.

Each sample contains a timestamp made up of seconds into the year and nanoseconds, as well as the value
of the PV at that timestamp. One PB file can contain many millions of samples. You can read more about
the Epics Archiver Appliance PB file structure here: [epicsarchiver.readthedocs](https://epicsarchiver.readthedocs.io/en/latest/developer/pb_pbraw.html).

Knowing this file structure and the PB format is essential to successfully reading and writing archiver
appliance PB files.
