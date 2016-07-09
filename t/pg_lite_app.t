use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use Mojo::IOLoop;
use Mojolicious::Lite;
use Test::Mojo;
use Mojo::JSON 'encode_json';

# Missing backend
eval { plugin Minion => {Something => 'fun'} };
like $@, qr/^Backend "Minion::Backend::Something" missing/, 'right error';

# Isolate tests
require Mojo::Pg;
my $pg = Mojo::Pg->new($ENV{TEST_ONLINE});
$pg->db->query('drop schema if exists minion_app_test cascade');
$pg->db->query('create schema minion_app_test');
plugin Minion => {Pg => $ENV{TEST_ONLINE}};
app->minion->backend->pg->search_path(['minion_app_test']);

app->minion->add_task(
  add => sub {
    my ($job, $first, $second) = @_;
    Mojo::IOLoop->next_tick(
      sub {
        $job->finish($first + $second);
        Mojo::IOLoop->stop;
      }
    );
    Mojo::IOLoop->start;
  }
);

my $queue = "test";

get '/add' => sub {
  my $c  = shift;
  $c->app->log->info("queue: $queue");
  my $id = $c->minion->enqueue(
    add => [$c->param('first'), $c->param('second')] => {queue => $queue});
  $c->render(text => $id);
};

get '/result' => sub {
  my $c = shift;
  $c->render(text => $c->minion->job($c->param('id'))->info->{result});
};

my $t = Test::Mojo->new;

my $worker = $t->app->minion->worker;

# Perform jobs automatically
$t->get_ok('/add' => form => {first => 1, second => 2})->status_is(200);
$t->app->minion->perform_jobs({queues => [$queue]});
$t->get_ok('/result' => form => {id => $t->tx->res->text})->status_is(200)
  ->content_is('3');
$t->get_ok('/add' => form => {first => 2, second => 3})->status_is(200);
my $first = $t->tx->res->text;
$t->get_ok('/add' => form => {first => 4, second => 5})->status_is(200);
my $second = $t->tx->res->text;
Mojo::IOLoop->delay(sub {
  while (my $job = $worker->register->dequeue(0, {queues => [$queue]})) { $job->perform }
})->wait;
$t->get_ok('/result' => form => {id => $first})->status_is(200)
  ->content_is('5');
$t->get_ok('/result' => form => {id => $second})->status_is(200)
  ->content_is('9');

# Msg to change queue
$worker->msg($worker->id, 'topic', 'queue', encode_json(['moar_test']));
my $msg = $worker->msg;
$queue = $msg->{args}[0];

# Use new queue
$t->get_ok('/add' => form => {first => 6, second => 1})->status_is(200);
my $third = $t->tx->res->text;
Mojo::IOLoop->delay(sub {
  while (my $job = $worker->register->dequeue(0, {queues => [$queue]})) { $job->perform }
})->wait;
$t->get_ok('/result' => form => {id => $third})->status_is(200)
  ->content_is('7');

is $t->app->minion->job($third)->info->{queue}, 'moar_test', 'right new queue';

$worker->unregister;

# Clean up once we are done
$pg->db->query('drop schema minion_app_test cascade');

done_testing();
