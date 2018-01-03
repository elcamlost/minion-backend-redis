use Mojo::Redis2::Server;
 
{
  my $server = Mojo::Redis2::Server->new;
  $server->start;
  while(1) { sleep 2; }# server runs here
}
 
