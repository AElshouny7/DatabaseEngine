# Description

The project involved building a database engine in Java that supports various functionalities, including creating tables, inserting tuples, deleting tuples, searching in tables linearly, creating Octrees upon demand, and using Octrees for efficient data retrieval.

Key Features:

* Implemented table storage as pages on disk, with each page representing a separate file.
* Supported data types for table columns: Integer, String, Double, and Date.
* Implemented a predetermined fixed maximum number of rows per page.
* Utilized Java's binary object file for emulating a page, storing tuples as separate objects within the file.
* Implemented lazy loading of pages to avoid loading the entire table's content into memory.
* Created a meta-data file to store information about tables, including column details, clustering key, and indices.
* Used Octrees as the indexing structure, supporting three dimensions.
* Implemented methods for creating tables, creating Octree indices, inserting rows, updating rows, and deleting rows.
* Enabled the use of indices for efficient query execution.
* Managed multiple submissions with different requirements, including the integration of indices.

The project enhanced knowledge and skills in database management, Java programming, file handling, serialization, and index structures.
