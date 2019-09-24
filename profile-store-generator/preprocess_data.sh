#!/usr/bin/env bash

# Get all artist IDs where the artist has more than 100 total listening events
awk '{print $2}' LFM-1b_LEs.txt | sort | uniq -c | awk '$1 > 100 {print $2}' > artist_ids_at_least_100_events.txt

# Get all listening events where the artist ID is included in the filtered ID file
perl -lane 'BEGIN{open(A,"artist_ids_at_least_100_events.txt"); while(<A>){chomp; $k{$_}++}}
            print $_ if defined($k{$F[1]}); ' LFM-1b_LEs.txt > LFM-1b_LES-filtered.txt

# Get all albums where the artist ID is included in the filtered ID file
perl -lane 'BEGIN{open(A,"artist_ids_at_least_100_events.txt"); while(<A>){chomp; $k{$_}++}}
            print $_ if defined($k{$F[2]}); ' LFM-1b_albums.txt > LFM-1b_albums-filtered.txt

# Get all artists where the artist ID is included in the filtered ID file
perl -lane 'BEGIN{open(A,"artist_ids_at_least_100_events.txt"); while(<A>){chomp; $k{$_}++}}
            print $_ if defined($k{$F[0]}); ' LFM-1b_artists.txt > LFM-1b_artists-filtered.txt

# Get all tracks where the artist ID is included in the filtered ID file
perl -lane 'BEGIN{open(A,"artist_ids_at_least_100_events.txt"); while(<A>){chomp; $k{$_}++}}
            print $_ if defined($k{$F[2]}); ' LFM-1b_tracks.txt > LFM-1b_tracks-filtered.txt

# Get all user IDs contained in the filtered down listening events file
cut -f-1 LFM-1b_LEs-filtered.txt | sort -u > user_ids_filtered.txt
