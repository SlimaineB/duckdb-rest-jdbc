(function dsbuilder(attr) {


    let url = "jdbc:duckdb://";  // `jdbc:duckdb:` or `jdbc:duckdb::memory:` are equivalent
    url +=  String(attr["hostport"]);

    return [url];
})
