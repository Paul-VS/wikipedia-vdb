import pstats

# Create a `Stats` object
stats = pstats.Stats('output.dat')

# You can sort the statistics by various types of column
stats.sort_stats('cumtime')

# This will print out the stats
stats.print_stats()
