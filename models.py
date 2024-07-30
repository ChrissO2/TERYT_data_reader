from django.db import models


class Province(models.Model):
    code = models.CharField(max_length=2)
    name = models.CharField(max_length=100)
    valid_date = models.DateField()

    def __str__(self):
        return f"{self.name} ({self.code})"


class County(models.Model):
    code = models.CharField(max_length=2)
    province = models.ForeignKey(Province, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    valid_date = models.DateField()

    def __str__(self):
        return f"{self.name} ({self.code})"


class Parish(models.Model):
    PARISH_TYPE_CHOICES = [
        (1, 'gmina miejska'),
        (2, 'gmina wiejska'),
        (3, 'gmina miejsko-wiejska'),
        (4, 'miasto w gminie miejsko-wiejskiej'),
        (5, 'obszar wiejski w gminie miejsko-wiejskie)'),
        (8, 'Ddzielnica w m.st. Warszawa'),
        (9, 'Delegatury miast Kraków, Łódź, Poznań, Wrocław'),
    ]
    code = models.CharField(max_length=2)
    county = models.ForeignKey(County, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    parish_type = models.IntegerField(choices=PARISH_TYPE_CHOICES)
    unit_type = models.CharField(max_length=50, blank=True, null=True)
    valid_date = models.DateField()

    def __str__(self):
        return f"{self.name} ({self.code})"


class PostalCode(models.Model):
    code = models.CharField(max_length=6, unique=True)

    def __str__(self):
        return self.code


class ParishPostalCode(models.Model):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    postal_code = models.ForeignKey(PostalCode, on_delete=models.CASCADE)

    def __str__(self):
        return f"{self.parish} - {self.postal_code}"


class City(models.Model):
    id = models.CharField(max_length=7, primary_key=True)
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    primary_city_id = models.CharField(max_length=7)
    valid_date = models.DateField()

    def __str__(self):
        return f"{self.name} ({self.id})"


class Street(models.Model):
    name_id = models.CharField(max_length=5)
    city = models.ForeignKey(City, on_delete=models.CASCADE)
    name1 = models.CharField(max_length=100)
    name2 = models.CharField(max_length=100, blank=True, null=True)
    street_type = models.CharField(max_length=5, blank=True, null=True)
    valid_date = models.DateField()

    def __str__(self):
        return f"{self.name1} {self.name2 or ''} ({self.name_id}) in {self.city}"
