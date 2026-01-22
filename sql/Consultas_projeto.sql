
-- produtos com maior faturamento
SELECT  produto, ROUND(SUM(faturamento_total),2) AS faturamento_total FROM vendas
GROUP BY produto
ORDER BY faturamento_total DESC
LIMIT 5;


-- clientes que mais gastaram
SELECT c.id_cliente, c.nome_cliente, ROUND(SUM(v.faturamento_total),2) AS valor_gasto FROM clientes c
JOIN vendas v ON c.id_cliente = v.id_cliente
GROUP BY c.id_cliente, c.nome_cliente
ORDER BY valor_gasto DESC
LIMIT 5;


-- faturamento médio plano de assinatura
SELECT
    plano_assinatura,
    ROUND(AVG(media_cliente), 2) AS faturamento_medio
FROM (
    SELECT
        c.id_cliente,
        c.plano_assinatura,
        AVG(v.faturamento_total) AS media_cliente
    FROM vendas v
    JOIN clientes c ON v.id_cliente = c.id_cliente
    GROUP BY c.id_cliente, c.plano_assinatura
) t
GROUP BY plano_assinatura;

