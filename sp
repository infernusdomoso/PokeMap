if [ -z $1 ]; then 
  echo "Scan Radius not set, using 10";
  SR=8
else
  SR="$1"
fi

if [ -z $2 ]; then 
  echo "Search location not set, using 39.5213122452,-76.643452323 (near York Rd)";
  SEARCH_LOC="39.5213122452,-76.643452323"  
else
  SEARCH_LOC="$2"
fi

screen python ./runserver.py --cors --auth-service ptc --port 18222 --db-type mysql --db-host localhost --db-name pokemon --db-user ub --db-pass kusa0213 --host 0.0.0.0 --gmaps-key AIzaSyAwQCf7euZqLhCNbOyg5QM_F_AOYiA1LMk -u fred1231231231 -u fred1231231232 -u fred123123123123 -u fred1231231233 -u fred1231231234 -u fred1231231235 --password ferret6 --step-limit $SR -l "$SEARCH_LOC"
