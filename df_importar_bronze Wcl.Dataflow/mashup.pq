[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "humanresources department_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"humanresources department" = let
  Origen = PostgreSQL.Database("wcalcagno.postgres.database.azure.com", "adventureworks"),
  #"Navegación 1" = Origen{[Schema = "humanresources", Item = "department"]}[Data],
  #"Se han quitado otras columnas." = Table.SelectColumns(#"Navegación 1", {"departmentid", "groupname", "name"})
in
  #"Se han quitado otras columnas.";
shared #"humanresources department_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "fb0905fd-76e1-4862-81bf-a8ad86ea00d6"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "fef4e400-36b2-4048-95d2-c1eb2197b4c8"]}[Data],
  TableNavigation = Navigation_2{[Id = "humanresources department", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
