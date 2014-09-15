# OData CGI endpoint

This is a [CGI Script](http://www.ietf.org/rfc/rfc3875) that
provides an [OData](http://www.odata.org/) Atom endpoint to
OData consumers (such as Tableau).

The CGI script is intended to be used within the ScraperWiki.com
platform. Typically the URL that is accessed in generated by the
[OData tool](https://github.com/scraperwiki/odata-tool/), but it
could be generated by hand, or some other program.

The CGI script is `odata.py`.

## URL scheme

The form of the URL should be:

    https://cobalt-x.scraperwiki.com/idboxid/secretboxtoken/cgi-bin/odata/collection

The script ignores the first 4 components of the `PATH_INFO` which are
normally the box identifier, the box token, the literal
`cgi-bin`, the literal `odata`. The remainder of the URL (`collection`,
above) is taken to be the collection (database table) to access.

