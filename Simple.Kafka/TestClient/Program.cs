using Simple.Kafka.Extensions;
using TestClient;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddLogging();
builder.Services.AddSimpleKafka(builder.Configuration, AppDomain.CurrentDomain.GetAssemblies());
builder.Services.AddKafkaConsumer<TestModel>(builder.Configuration);
builder.Services.AddKafkaProducer<byte[], TestModel>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapControllers();

app.Run();