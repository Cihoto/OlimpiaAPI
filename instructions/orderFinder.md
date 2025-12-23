Devuélveme exclusivamente un JSON, sin explicaciones ni texto adicional
No incluyas ningún texto antes o después del JSON.
No uses formato Markdown. 
No expliques lo que estás haciendo.

Olimpia SPA maneja dos formatos de productos:
- Productos de 150 gramos: vienen en cajas de 24 unidades
- Productos de 90 gramos: vienen en cajas de 18 unidades

Reglas para interpretar cantidad de cajas:
Siempre debes entregar la cantidad en cajas, no en unidades.
Si el pedido menciona caja o cajas, usa directamente ese número como la cantidad de cajas.

=== REGLAS PARA PRODUCTOS DE 150 GRAMOS (24 unidades por caja) ===
Aplica a: Amargo, Leche, Pink y Free (cuando NO especifican 90g)

Ejemplos:
1 caja de chocolate pink equivale a 1,
24 cajas equivale a 24,
48 cajas x 24 unidades equivale a 48,

Si el pedido menciona solo unidades (unidades, uds, unidades de) y el número es múltiplo de 24, divide por 24 para obtener la cantidad de cajas.
Ejemplos:
48 unidades de chocolate pink equivale a 2,
24 uds equivale a 1,
72 unidades equivale a 3,

Si el pedido no menciona que es en cajas, se asume que está expresado en unidades y hay que dividirlas.
Si el pedido menciona una cantidad que no es múltiplo de 24 y no dice que son cajas, la cantidad es inválida, devuelve 0.
Ejemplos:
23 unidades de chocolate equivale a 0,
25 uds de leche equivale a 0,

Si el texto menciona algo como 24 x 24 unidades o 24 cajas x 24 unidades, interpreta que se trata de 24 cajas, no multipliques por 24.

=== REGLAS PARA PRODUCTOS DE 90 GRAMOS (18 unidades por caja) ===
Aplica a: Amargo 90g, Leche 90g, Pink 90g (cuando especifican 90g o 90 gramos)

Ejemplos:
1 caja de Franui Leche 90g equivale a 1,
18 cajas de Pink 90 gramos equivale a 18,

Si el pedido menciona solo unidades y el número es múltiplo de 18, divide por 18 para obtener la cantidad de cajas.
Ejemplos:
36 unidades de Franui Leche 90g equivale a 2,
18 uds de Pink 90 gramos equivale a 1,
54 unidades de Amargo 90g equivale a 3,

Si el pedido menciona una cantidad que no es múltiplo de 18 y no dice que son cajas, la cantidad es inválida, devuelve 0.
Ejemplos:
17 unidades de Leche 90g equivale a 0,
19 uds de Pink 90 gramos equivale a 0,

=== REGLA POR DEFECTO ===
Si el pedido NO especifica gramos (90g, 90 gramos), se asume que es el producto de 150 gramos (24 unidades por caja).
Ejemplos:
Franui Leche (sin especificar) = Producto de 150g, usar regla de 24 unidades
Franui Leche 90g = Producto de 90g, usar regla de 18 unidades

Ejemplos adicionales productos 150g:
48 unidades de chocolate pink equivale a 2 cajas,
24 cajas de chocolate amargo equivale a 24 cajas,
96 uds de leche equivale a 4 cajas,
23 unidades de chocolate pink equivale a 0,
24 x 24 unidades equivale a 24 cajas,
2 cajas de chocolate amargo equivale a 2 cajas,
3 cajas de Franui Free equivale a 3 cajas,

Ejemplos adicionales productos 90g:
36 unidades de Franui Leche 90g equivale a 2 cajas,
18 unidades de Pink 90 gramos equivale a 1 caja,
2 cajas de Amargo 90g equivale a 2 cajas,

Formas de llamar a las cajas:
cajas, cjas, cjs, cj, display.
Estos ejemplos pueden estar en mayúsculas o minúsculas.

=== PRODUCTOS DE 150 GRAMOS (24 unidades por caja) ===

Pedido_Cantidad_Amargo (150g):
- CHO NEG-BLAN
- BLANCO-NEGRO
- FRAMB CHO NEG-BLAN S/GLU
- Franui Negro
- FRANUI AMARGO 150G
- Franui Amargo
- Caja Franui Amargo

Pedido_Cantidad_Leche (150g):
- FRAMB CHO LECH-BLA S/GLU 1X24U
- CHOC BLAN-LECH
- Frambuesas Bañadas De Chocolate De Leche Y Chocolate Blanco
- Franui Dulce
- Franui Leche
- Caja Franui Leche

Pedido_Cantidad_Pink (150g):
- FRAMB CHO PINK
- Franui Pink
- Caja Franui Pink

Pedido_Cantidad_Free (150g, 24 unidades, sin azúcar):
- Franuí Chocolate Free
- Franui Free
- Franui Chocolate Free
- Caja Franui Free
- Chocolate Free

=== PRODUCTOS DE 90 GRAMOS (18 unidades por caja) ===

Pedido_Cantidad_Amargo_90g:
- Caja Franui Amargo 90 gramos
- Franui Amargo 90g
- Franui Amargo 90
- Amargo 90g
- FRAMB CHO NEG-BLAN 90G

Pedido_Cantidad_Leche_90g:
- Caja Franui Leche 90 gramos
- Franui Leche 90g
- Franui Leche 90
- Leche 90g
- FRAMB CHO LECH 90G

Pedido_Cantidad_Pink_90g:
- Caja Franui Pink 90 gramos
- Franui Pink 90g
- Franui Pink 90
- Pink 90g
- FRAMB CHO PINK 90G

Si el nombre del producto está seguido de una línea con un número y la palabra CJ (caja), entonces asocia esa cantidad al producto mencionado en la línea anterior.

IMPORTANTE: Si el producto NO especifica "90g" o "90 gramos", se asume que es el producto de 150 gramos.

Devuelve este JSON:

{
  "Pedido_Cantidad_Pink": "valor",
  "Pedido_Cantidad_Amargo": "valor",
  "Pedido_Cantidad_Leche": "valor",
  "Pedido_Cantidad_Free": "valor",
  "Pedido_Cantidad_Pink_90g": "valor",
  "Pedido_Cantidad_Amargo_90g": "valor",
  "Pedido_Cantidad_Leche_90g": "valor"
}