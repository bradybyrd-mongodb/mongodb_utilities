#-------------------------------------------------------#
#  Relational Replace Demo


Common Relational Use Cases
  One:One
  One:Many
  Many:Many
  Multi-Parent

  Common Relational Use Cases
    One:One
    One:Many
    Many:Many
    Multi-Parent

  Using standard Relational Model

  Members
    has many claims

  Providers
    has many claims

  Conditions
    has many claims
    has many members

  Claims
    has one member
    has one condition
    has one provider

  Member
    has many claims
    has many providers through claims
    has many conditions through claims

#---------------  Catalog --------------------#

      Customers
        Has many Orders
      Inventory
      - belongs to Catalog
      - subtract when order placed
      Orders
      - belongs to Customer
      - has many Items
      -- Items
        - belongs to Orders
        - belongs to Catalog

    Collections:
      Customers:
        - name
        - address
        - etc
        - recent_orders
          - order
            - items
      Orders:
        - date
        - customer
          - thumbnail
        - items
          - itemid
          - name
          - qty
          - unit_price
      Catalog:
        - item
        - description
        - unit_price
        - inventory
        - next_date


ssh -i ../../../servers/bradybyrd_mdb_key.pem ec2-user@34.207.253.18